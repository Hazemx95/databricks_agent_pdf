# Databricks notebook source
"""Parse available PDF binaries with Databricks-native document parsing.

Runs inside Databricks through MCP. This notebook stops at PDF parsing: it does
not run ai_extract/ai_query extraction, validate AI JSON, or write final
extraction results.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from pyspark.sql import Row
from pyspark.sql import functions as F
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
PARSED_TABLE = "databricks_arrow_cata.main.pdf_parsed_documents"
AUDIT_TABLE = "databricks_arrow_cata.main.pdf_extraction_audit"
RAW_PDFS_DIR = "/Volumes/databricks_arrow_cata/main/pdf_ai/raw_pdfs/"

STATUS_DOWNLOADED = "DOWNLOADED"
STATUS_MANUALLY_STAGED = "MANUALLY_STAGED"

PARSE_STATUS_PARSED = "PARSED"
PARSE_STATUS_FAILED = "PARSE_FAILED"
PARSE_STATUS_EMPTY = "EMPTY_DOCUMENT"
STEP_PARSE_DOCUMENT = "PARSE_DOCUMENT"


@dataclass(frozen=True)
class ParseCandidate:
    doc_id: int
    cleaned_url: str
    local_pdf_path: str
    source_file_name: Optional[str]
    supplier_folder: Optional[str]
    url_part_hint: Optional[str]
    input_status: str


@dataclass(frozen=True)
class ParseOutcome:
    doc_id: int
    cleaned_url: str
    local_pdf_path: str
    parsed_content: Optional[str]
    parse_status: str
    parse_error: Optional[str]
    started_at: datetime
    finished_at: datetime


def utcnow() -> datetime:
    return datetime.utcnow()


def configured_batch_size() -> int:
    configured_size = "5"
    if "dbutils" in globals():
        dbutils.widgets.text("batch_size", "5")
        configured_size = dbutils.widgets.get("batch_size")
    return max(1, min(int(configured_size), 5))


def file_name(path: str) -> str:
    return os.path.basename(path or "")


def list_available_binary_files():
    return (
        spark.read.format("binaryFile")
        .load(RAW_PDFS_DIR)
        .select("path", "content", "length", "modificationTime")
        .where((F.col("length").isNotNull()) & (F.col("length") > 0))
        .withColumn("binary_file_name", F.element_at(F.split(F.col("path"), "/"), -1))
    )


def candidate_rows(limit_count: int) -> list[ParseCandidate]:
    binary_files = list_available_binary_files()
    control_rows = (
        spark.table(CONTROL_TABLE)
        .where(
            (F.col("input_status").isin(STATUS_DOWNLOADED, STATUS_MANUALLY_STAGED))
            & F.col("local_pdf_path").isNotNull()
        )
        .select(
            "doc_id",
            "cleaned_url",
            "local_pdf_path",
            "source_file_name",
            "supplier_folder",
            "url_part_hint",
            "input_status",
        )
        .withColumn("local_pdf_file_name", F.element_at(F.split(F.col("local_pdf_path"), "/"), -1))
    )

    matched = (
        control_rows.join(
            binary_files,
            (control_rows.local_pdf_path == binary_files.path)
            | (control_rows.local_pdf_file_name == binary_files.binary_file_name),
            "inner",
        )
        .select(
            "doc_id",
            "cleaned_url",
            F.col("path").alias("local_pdf_path"),
            "source_file_name",
            "supplier_folder",
            "url_part_hint",
            "input_status",
        )
        .dropDuplicates(["doc_id"])
        .orderBy("doc_id")
        .limit(limit_count)
    )

    return [
        ParseCandidate(
            doc_id=int(row.doc_id),
            cleaned_url=row.cleaned_url,
            local_pdf_path=row.local_pdf_path,
            source_file_name=row.source_file_name,
            supplier_folder=row.supplier_folder,
            url_part_hint=row.url_part_hint,
            input_status=row.input_status,
        )
        for row in matched.collect()
    ]


def parse_error_is_meaningful(parse_error: Optional[str]) -> bool:
    if parse_error is None:
        return False
    normalized = parse_error.strip().lower()
    return normalized not in {"", "null", "[]", "{}"}


def parsed_content_is_empty(parsed_content: Optional[str]) -> bool:
    if parsed_content is None:
        return True
    normalized = parsed_content.strip().lower()
    return normalized in {"", "null", "[]", "{}"}


def parse_candidate(candidate: ParseCandidate) -> ParseOutcome:
    started_at = utcnow()
    try:
        parsed_row = (
            spark.read.format("binaryFile")
            .load(candidate.local_pdf_path)
            .withColumn("parsed", F.expr("ai_parse_document(content, map('version', '2.0'))"))
            .selectExpr(
                "CAST(parsed AS STRING) AS parsed_content",
                "CAST(parsed:error AS STRING) AS parse_error",
                "CAST(parsed:error_status AS STRING) AS error_status",
            )
            .limit(1)
            .collect()[0]
        )
        parsed_content = parsed_row.parsed_content
        parse_error = parsed_row.parse_error or parsed_row.error_status
        if parse_error_is_meaningful(parse_error):
            parse_status = PARSE_STATUS_FAILED
        elif parsed_content_is_empty(parsed_content):
            parse_status = PARSE_STATUS_EMPTY
        else:
            parse_status = PARSE_STATUS_PARSED
            parse_error = None
    except Exception as exc:
        parsed_content = None
        parse_status = PARSE_STATUS_FAILED
        parse_error = str(exc)[:2000]

    return ParseOutcome(
        doc_id=candidate.doc_id,
        cleaned_url=candidate.cleaned_url,
        local_pdf_path=candidate.local_pdf_path,
        parsed_content=parsed_content,
        parse_status=parse_status,
        parse_error=parse_error,
        started_at=started_at,
        finished_at=utcnow(),
    )


def parsed_schema() -> StructType:
    return StructType(
        [
            StructField("doc_id", LongType(), False),
            StructField("cleaned_url", StringType(), True),
            StructField("local_pdf_path", StringType(), True),
            StructField("parsed_content", StringType(), True),
            StructField("parse_status", StringType(), False),
            StructField("parse_error", StringType(), True),
            StructField("parsed_at", TimestampType(), False),
        ]
    )


def audit_schema() -> StructType:
    return StructType(
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


def write_parse_outcomes(outcomes: list[ParseOutcome]) -> None:
    if not outcomes:
        return

    rows = [
        Row(
            doc_id=outcome.doc_id,
            cleaned_url=outcome.cleaned_url,
            local_pdf_path=outcome.local_pdf_path,
            parsed_content=outcome.parsed_content,
            parse_status=outcome.parse_status,
            parse_error=outcome.parse_error,
            parsed_at=outcome.finished_at,
        )
        for outcome in outcomes
    ]
    spark.createDataFrame(rows, parsed_schema()).createOrReplaceTempView("pdf_parse_updates")
    spark.sql(
        f"""
        MERGE INTO {PARSED_TABLE} AS target
        USING pdf_parse_updates AS source
        ON target.doc_id = source.doc_id
        WHEN MATCHED THEN UPDATE SET
          target.cleaned_url = source.cleaned_url,
          target.local_pdf_path = source.local_pdf_path,
          target.parsed_content = source.parsed_content,
          target.parse_status = source.parse_status,
          target.parse_error = source.parse_error,
          target.parsed_at = source.parsed_at
        WHEN NOT MATCHED THEN INSERT (
          doc_id, cleaned_url, local_pdf_path, parsed_content,
          parse_status, parse_error, parsed_at
        ) VALUES (
          source.doc_id, source.cleaned_url, source.local_pdf_path, source.parsed_content,
          source.parse_status, source.parse_error, source.parsed_at
        )
        """
    )


def write_audit_rows(outcomes: list[ParseOutcome]) -> None:
    if not outcomes:
        return

    rows = []
    for outcome in outcomes:
        rows.append(
            Row(
                doc_id=outcome.doc_id,
                cleaned_url=outcome.cleaned_url,
                step_name=STEP_PARSE_DOCUMENT,
                status=outcome.parse_status,
                error_message=outcome.parse_error,
                retry_count=0,
                started_at=outcome.started_at,
                finished_at=outcome.finished_at,
                duration_seconds=(outcome.finished_at - outcome.started_at).total_seconds(),
                model_endpoint=None,
                created_at=outcome.finished_at,
            )
        )

    spark.createDataFrame(rows, audit_schema()).write.mode("append").saveAsTable(AUDIT_TABLE)


batch_size = configured_batch_size()
selected_candidates = candidate_rows(batch_size)
parse_outcomes = [parse_candidate(candidate) for candidate in selected_candidates]

write_parse_outcomes(parse_outcomes)
write_audit_rows(parse_outcomes)

metadata_rows = [
    Row(
        doc_id=candidate.doc_id,
        cleaned_url=candidate.cleaned_url,
        local_pdf_path=candidate.local_pdf_path,
        source_file_name=candidate.source_file_name,
        supplier_folder=candidate.supplier_folder,
        url_part_hint=candidate.url_part_hint,
        input_status=candidate.input_status,
    )
    for candidate in selected_candidates
]
if metadata_rows:
    spark.createDataFrame(metadata_rows).createOrReplaceTempView("phase7_parse_selected_metadata")

summary = {
    "raw_pdf_folder": RAW_PDFS_DIR,
    "configured_batch_size": batch_size,
    "available_pdfs_selected_for_parsing": len(selected_candidates),
    "parse_rows_written_or_updated": len(parse_outcomes),
    "parsed_successfully": sum(1 for outcome in parse_outcomes if outcome.parse_status == PARSE_STATUS_PARSED),
    "parse_failures": sum(1 for outcome in parse_outcomes if outcome.parse_status == PARSE_STATUS_FAILED),
    "empty_documents": sum(1 for outcome in parse_outcomes if outcome.parse_status == PARSE_STATUS_EMPTY),
    "ai_parse_document_run": True,
    "ai_extract_run": False,
    "ai_json_validated": False,
    "final_result_rows_written": 0,
    "stopped_before_next_phase": True,
}

print("PHASE_PARSE_DOCUMENTS_SUMMARY=" + json.dumps(summary, sort_keys=True, default=str))

display(
    spark.table(PARSED_TABLE)
    .where(F.col("doc_id").isin([candidate.doc_id for candidate in selected_candidates] or [-1]))
    .select("doc_id", "cleaned_url", "local_pdf_path", "parse_status", "parse_error", "parsed_at")
    .orderBy("doc_id")
)

display(
    spark.table(AUDIT_TABLE)
    .where(F.col("step_name") == STEP_PARSE_DOCUMENT)
    .select(
        "doc_id",
        "cleaned_url",
        "step_name",
        "status",
        "error_message",
        "retry_count",
        "started_at",
        "finished_at",
        "duration_seconds",
        "model_endpoint",
        "created_at",
    )
    .orderBy(F.col("created_at").desc())
    .limit(20)
)
