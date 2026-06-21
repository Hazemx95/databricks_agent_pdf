# Databricks notebook source
"""Acquire a small POC batch of PDFs into the raw PDF Volume folder.

Runs inside Databricks. This notebook only downloads PDFs or matches already
staged PDFs; it does not read PDFs with binaryFile, parse documents, run AI
extraction, or write final extraction results.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from pyspark.sql import Row
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


CONTROL_TABLE = "databricks_arrow_cata.main.pdf_input_urls"
AUDIT_TABLE = "databricks_arrow_cata.main.pdf_extraction_audit"
RAW_PDFS_DIR = "/Volumes/databricks_arrow_cata/main/pdf_ai/raw_pdfs/"

STATUS_NEW = "NEW"
STATUS_DOWNLOADING = "DOWNLOADING"
STATUS_DOWNLOADED = "DOWNLOADED"
STATUS_DOWNLOAD_FAILED = "DOWNLOAD_FAILED"
STATUS_MANUALLY_STAGED = "MANUALLY_STAGED"

STEP_DOWNLOAD_PDF = "DOWNLOAD_PDF"
STEP_STAGE_PDF = "STAGE_PDF"


@dataclass(frozen=True)
class ControlRow:
    doc_id: int
    cleaned_url: str
    source_file_name: str
    url_part_hint: Optional[str]


@dataclass(frozen=True)
class AcquisitionOutcome:
    doc_id: int
    cleaned_url: str
    input_status: str
    local_pdf_path: Optional[str]
    error_message: Optional[str]
    step_name: str
    started_at: datetime
    finished_at: datetime


def utcnow() -> datetime:
    return datetime.utcnow()


def batch_size() -> int:
    configured_size = "1"
    if "dbutils" in globals():
        dbutils.widgets.text("batch_size", "1")
        configured_size = dbutils.widgets.get("batch_size")
    return max(1, min(int(configured_size), 5))


def selected_new_rows(limit_count: int) -> list[ControlRow]:
    rows = spark.sql(
        f"""
        SELECT doc_id, cleaned_url, source_file_name, url_part_hint
        FROM {CONTROL_TABLE}
        WHERE input_status = '{STATUS_NEW}'
        ORDER BY doc_id
        LIMIT {limit_count}
        """
    ).collect()
    return [
        ControlRow(
            doc_id=int(row.doc_id),
            cleaned_url=row.cleaned_url,
            source_file_name=row.source_file_name,
            url_part_hint=row.url_part_hint,
        )
        for row in rows
    ]


def staging_candidate_rows() -> list[ControlRow]:
    rows = spark.sql(
        f"""
        SELECT doc_id, cleaned_url, source_file_name, url_part_hint
        FROM {CONTROL_TABLE}
        WHERE input_status IN ('{STATUS_NEW}', '{STATUS_DOWNLOAD_FAILED}')
          AND local_pdf_path IS NULL
        ORDER BY doc_id
        """
    ).collect()
    return [
        ControlRow(
            doc_id=int(row.doc_id),
            cleaned_url=row.cleaned_url,
            source_file_name=row.source_file_name,
            url_part_hint=row.url_part_hint,
        )
        for row in rows
    ]


def deterministic_pdf_name(control_row: ControlRow) -> str:
    import hashlib

    url_hash = hashlib.sha256(control_row.cleaned_url.encode("utf-8")).hexdigest()[:16]
    source_file_name = os.path.basename(control_row.source_file_name or f"doc_{control_row.doc_id}.pdf")
    return f"{control_row.doc_id}_{url_hash}_{source_file_name}"


def raw_pdf_path(file_name: str) -> str:
    return RAW_PDFS_DIR.rstrip("/") + "/" + file_name


def list_staged_pdf_names() -> list[str]:
    try:
        names = os.listdir(RAW_PDFS_DIR)
    except FileNotFoundError:
        return []
    return sorted(name for name in names if name.lower().endswith(".pdf"))


def update_status(doc_id: int, input_status: str, local_pdf_path: Optional[str], error_message: Optional[str]) -> None:
    update_schema = StructType(
        [
            StructField("doc_id", LongType(), False),
            StructField("input_status", StringType(), False),
            StructField("local_pdf_path", StringType(), True),
            StructField("error_message", StringType(), True),
        ]
    )
    spark.createDataFrame(
        [Row(doc_id=doc_id, input_status=input_status, local_pdf_path=local_pdf_path, error_message=error_message)],
        update_schema,
    ).createOrReplaceTempView("pdf_acquire_update")
    spark.sql(
        f"""
        MERGE INTO {CONTROL_TABLE} AS target
        USING pdf_acquire_update AS source
        ON target.doc_id = source.doc_id
        WHEN MATCHED THEN UPDATE SET
          target.input_status = source.input_status,
          target.local_pdf_path = source.local_pdf_path,
          target.error_message = source.error_message,
          target.updated_at = current_timestamp()
        """
    )


def append_audit(outcome: AcquisitionOutcome) -> None:
    audit_schema = StructType(
        [
            StructField("doc_id", LongType(), True),
            StructField("cleaned_url", StringType(), True),
            StructField("step_name", StringType(), False),
            StructField("status", StringType(), False),
            StructField("error_message", StringType(), True),
            StructField("retry_count", IntegerType(), True),
            StructField("started_at", TimestampType(), False),
            StructField("finished_at", TimestampType(), False),
            StructField("duration_seconds", DoubleType(), False),
            StructField("model_endpoint", StringType(), True),
            StructField("created_at", TimestampType(), False),
        ]
    )
    duration_seconds = (outcome.finished_at - outcome.started_at).total_seconds()
    audit_row = Row(
        doc_id=outcome.doc_id,
        cleaned_url=outcome.cleaned_url,
        step_name=outcome.step_name,
        status=outcome.input_status,
        error_message=outcome.error_message,
        retry_count=0,
        started_at=outcome.started_at,
        finished_at=outcome.finished_at,
        duration_seconds=duration_seconds,
        model_endpoint=None,
        created_at=outcome.finished_at,
    )
    spark.createDataFrame([audit_row], audit_schema).write.mode("append").saveAsTable(AUDIT_TABLE)


def download_pdf(control_row: ControlRow) -> AcquisitionOutcome:
    started_at = utcnow()
    file_path = raw_pdf_path(deterministic_pdf_name(control_row))
    update_status(control_row.doc_id, STATUS_DOWNLOADING, None, None)
    if os.path.exists(file_path):
        return AcquisitionOutcome(
            control_row.doc_id,
            control_row.cleaned_url,
            STATUS_DOWNLOAD_FAILED,
            None,
            "Deterministic target already exists; not overwritten",
            STEP_DOWNLOAD_PDF,
            started_at,
            utcnow(),
        )
    try:
        request = urllib.request.Request(control_row.cleaned_url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(request, timeout=30) as response:
            with open(file_path, "wb") as target_file:
                target_file.write(response.read())
        input_status = STATUS_DOWNLOADED
        error_message = None
        local_pdf_path = file_path
    except (OSError, urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as exc:
        input_status = STATUS_DOWNLOAD_FAILED
        error_message = str(exc)[:1000]
        local_pdf_path = None

    return AcquisitionOutcome(
        control_row.doc_id,
        control_row.cleaned_url,
        input_status,
        local_pdf_path,
        error_message,
        STEP_DOWNLOAD_PDF,
        started_at,
        utcnow(),
    )


def matching_staged_pdf(control_row: ControlRow, staged_pdf_names: list[str]) -> Optional[str]:
    expected_name = deterministic_pdf_name(control_row)
    if expected_name in staged_pdf_names:
        return expected_name

    candidates = []
    source_file_name = os.path.basename(control_row.source_file_name or "")
    if source_file_name:
        candidates = [name for name in staged_pdf_names if name == source_file_name]
        if len(candidates) == 1:
            return candidates[0]
        candidates = [name for name in staged_pdf_names if source_file_name in name]
        if len(candidates) == 1:
            return candidates[0]

    url_part_hint = control_row.url_part_hint or ""
    if len(url_part_hint) >= 4:
        candidates = [name for name in staged_pdf_names if url_part_hint in name]
        if len(candidates) == 1:
            return candidates[0]

    return None


def stage_matched_pdf(control_row: ControlRow, staged_pdf_name: str) -> AcquisitionOutcome:
    started_at = utcnow()
    file_path = raw_pdf_path(staged_pdf_name)
    return AcquisitionOutcome(
        control_row.doc_id,
        control_row.cleaned_url,
        STATUS_MANUALLY_STAGED,
        file_path,
        None,
        STEP_STAGE_PDF,
        started_at,
        utcnow(),
    )


def unique_staged_matches(control_rows: list[ControlRow], staged_pdf_names: list[str]) -> list[tuple[ControlRow, str]]:
    proposed_matches = []
    for control_row in control_rows:
        staged_pdf_name = matching_staged_pdf(control_row, staged_pdf_names)
        if staged_pdf_name:
            proposed_matches.append((control_row, staged_pdf_name))

    staged_name_counts = {}
    for _, staged_pdf_name in proposed_matches:
        staged_name_counts[staged_pdf_name] = staged_name_counts.get(staged_pdf_name, 0) + 1

    return [
        (control_row, staged_pdf_name)
        for control_row, staged_pdf_name in proposed_matches
        if staged_name_counts[staged_pdf_name] == 1
    ]


def apply_outcome(outcome: AcquisitionOutcome) -> None:
    update_status(outcome.doc_id, outcome.input_status, outcome.local_pdf_path, outcome.error_message)
    append_audit(outcome)


configured_batch_size = batch_size()
batch_rows = selected_new_rows(configured_batch_size)
staged_pdf_names_before = list_staged_pdf_names()
download_outcomes = []
stage_outcomes = []
rows_needing_download = []

for control_row in batch_rows:
    staged_pdf_name = matching_staged_pdf(control_row, staged_pdf_names_before)
    if staged_pdf_name:
        outcome = stage_matched_pdf(control_row, staged_pdf_name)
        apply_outcome(outcome)
        stage_outcomes.append(outcome)
        continue

    rows_needing_download.append(control_row)
    outcome = download_pdf(control_row)
    apply_outcome(outcome)
    download_outcomes.append(outcome)

staged_pdf_names_after = list_staged_pdf_names()
already_staged_doc_ids = {outcome.doc_id for outcome in stage_outcomes}
available_stage_candidates = [
    control_row for control_row in staging_candidate_rows() if control_row.doc_id not in already_staged_doc_ids
]

for control_row, staged_pdf_name in unique_staged_matches(available_stage_candidates, staged_pdf_names_after):
    outcome = stage_matched_pdf(control_row, staged_pdf_name)
    apply_outcome(outcome)
    stage_outcomes.append(outcome)

matched_staged_names = {os.path.basename(outcome.local_pdf_path) for outcome in stage_outcomes if outcome.local_pdf_path}
selected_expected_names = {deterministic_pdf_name(row) for row in batch_rows}
selected_source_names = {os.path.basename(row.source_file_name or "") for row in batch_rows}
unmatched_staged_pdf_names = [
    name
    for name in staged_pdf_names_after
    if name not in matched_staged_names and name not in selected_expected_names and name not in selected_source_names
]

summary = {
    "selected_new_rows_for_poc_batch": len(batch_rows),
    "configured_batch_size": configured_batch_size,
    "downloaded_successfully": sum(1 for outcome in download_outcomes if outcome.input_status == STATUS_DOWNLOADED),
    "download_failures": sum(1 for outcome in download_outcomes if outcome.input_status == STATUS_DOWNLOAD_FAILED),
    "manually_staged_matched": len(stage_outcomes),
    "unmatched_staged_pdfs": unmatched_staged_pdf_names,
    "staged_pdf_count_before": len(staged_pdf_names_before),
    "staged_pdf_count_after": len(staged_pdf_names_after),
    "pdfs_read_as_binary": 0,
    "pdfs_parsed": 0,
    "ai_extractions_run": 0,
    "final_result_rows_written": 0,
}

print("PHASE_ACQUIRE_PDFS_SUMMARY=" + json.dumps(summary, sort_keys=True))

display(
    spark.sql(
        f"""
        SELECT doc_id, cleaned_url, source_file_name, input_status, local_pdf_path, error_message
        FROM {CONTROL_TABLE}
        WHERE doc_id IN ({','.join(str(row.doc_id) for row in batch_rows) or 'NULL'})
        ORDER BY doc_id
        """
    )
)
