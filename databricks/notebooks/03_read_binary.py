# Databricks notebook source
"""Read available raw PDFs as binary and join them to the control table.

Runs inside Databricks. This notebook intentionally stops at binary reading:
it does not call ai_parse_document, run AI extraction, validate AI JSON, or
write final extraction results.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from pyspark.sql import Row, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BinaryType,
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

STATUS_DOWNLOADED = "DOWNLOADED"
STATUS_MANUALLY_STAGED = "MANUALLY_STAGED"
STEP_READ_BINARY = "READ_BINARY"


@dataclass(frozen=True)
class ReadFailure:
    file_path: str
    error_message: str


def utcnow() -> datetime:
    return datetime.utcnow()


def list_raw_pdf_paths() -> list[str]:
    try:
        file_names = os.listdir(RAW_PDFS_DIR)
    except FileNotFoundError:
        return []
    return sorted(
        RAW_PDFS_DIR.rstrip("/") + "/" + name
        for name in file_names
        if name.lower().endswith(".pdf")
    )


def empty_binary_df():
    schema = StructType(
        [
            StructField("path", StringType(), False),
            StructField("content", BinaryType(), True),
            StructField("length", LongType(), True),
            StructField("modificationTime", TimestampType(), True),
        ]
    )
    return spark.createDataFrame([], schema)


def read_binary_files(file_paths: list[str]):
    binary_dfs = []
    failures = []
    for file_path in file_paths:
        try:
            binary_dfs.append(
                spark.read.format("binaryFile")
                .load(file_path)
                .select("path", "content", "length", "modificationTime")
            )
        except Exception as exc:
            failures.append(ReadFailure(file_path=file_path, error_message=str(exc)[:1000]))

    if not binary_dfs:
        return empty_binary_df(), failures

    result_df = binary_dfs[0]
    for next_df in binary_dfs[1:]:
        result_df = result_df.unionByName(next_df)
    return result_df, failures


def available_control_df():
    return (
        spark.table(CONTROL_TABLE)
        .where(
            (F.col("input_status").isin(STATUS_DOWNLOADED, STATUS_MANUALLY_STAGED))
            & F.col("local_pdf_path").isNotNull()
        )
        .select(
            "doc_id",
            "source_url",
            "cleaned_url",
            "source_file_name",
            "supplier_folder",
            "url_part_hint",
            "input_status",
            "local_pdf_path",
        )
    )


def file_name_col(path_col):
    return F.element_at(F.split(path_col, "/"), -1)


def matching_candidates(binary_df, control_df):
    files = binary_df.withColumnRenamed("path", "binary_file_path").withColumn(
        "binary_file_name", file_name_col(F.col("binary_file_path"))
    )
    controls = control_df.withColumn("local_pdf_file_name", file_name_col(F.col("local_pdf_path")))

    direct_path = F.col("binary_file_path") == F.col("local_pdf_path")
    direct_name = F.col("binary_file_name") == F.col("local_pdf_file_name")
    source_name = (
        F.col("source_file_name").isNotNull()
        & (F.col("source_file_name") != "")
        & (F.col("binary_file_name") == F.col("source_file_name"))
    )
    source_name_contained = (
        F.col("source_file_name").isNotNull()
        & (F.col("source_file_name") != "")
        & F.col("binary_file_name").contains(F.col("source_file_name"))
    )
    hint_contained = (
        F.col("url_part_hint").isNotNull()
        & (F.length("url_part_hint") >= 4)
        & F.col("binary_file_name").contains(F.col("url_part_hint"))
    )

    match_condition = direct_path | direct_name | source_name | source_name_contained | hint_contained
    return (
        files.join(F.broadcast(controls), match_condition, "inner")
        .withColumn(
            "match_priority",
            F.when(direct_path, F.lit(1))
            .when(direct_name, F.lit(2))
            .when(source_name, F.lit(3))
            .when(source_name_contained, F.lit(4))
            .otherwise(F.lit(5)),
        )
        .select(
            "doc_id",
            "source_url",
            "cleaned_url",
            "source_file_name",
            "supplier_folder",
            "url_part_hint",
            "input_status",
            "local_pdf_path",
            "binary_file_path",
            "binary_file_name",
            "content",
            F.col("length").alias("file_size"),
            "modificationTime",
            "match_priority",
        )
    )


def unambiguous_matches(candidates_df):
    ranked = candidates_df.withColumn(
        "best_rank",
        F.row_number().over(Window.partitionBy("binary_file_path").orderBy("match_priority", "doc_id")),
    )
    best = ranked.where(F.col("best_rank") == 1).drop("best_rank")
    ambiguity = (
        candidates_df.groupBy("binary_file_path", "match_priority")
        .agg(F.countDistinct("doc_id").alias("doc_matches_at_priority"))
        .where(F.col("doc_matches_at_priority") > 1)
        .select("binary_file_path", "match_priority")
    )
    return best.join(ambiguity, on=["binary_file_path", "match_priority"], how="left_anti")


def append_audit_rows(rows: list[Row]) -> None:
    if not rows:
        return
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
    spark.createDataFrame(rows, audit_schema).write.mode("append").saveAsTable(AUDIT_TABLE)


def audit_row(doc_id: Optional[int], cleaned_url: Optional[str], status: str, error_message: Optional[str], started_at):
    finished_at = utcnow()
    return Row(
        doc_id=doc_id,
        cleaned_url=cleaned_url,
        step_name=STEP_READ_BINARY,
        status=status,
        error_message=error_message,
        retry_count=0,
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=(finished_at - started_at).total_seconds(),
        model_endpoint=None,
        created_at=finished_at,
    )


phase_started_at = utcnow()
raw_pdf_paths = list_raw_pdf_paths()
binary_df, unreadable_failures = read_binary_files(raw_pdf_paths)
control_df = available_control_df()

candidates_df = matching_candidates(binary_df, control_df)
matched_df = unambiguous_matches(candidates_df).cache()
matched_df.createOrReplaceTempView("phase6_binary_read_result")

matched_doc_ids_df = matched_df.select("doc_id").distinct()
missing_control_df = control_df.join(matched_doc_ids_df, on="doc_id", how="left_anti")

matched_paths_df = matched_df.select("binary_file_path").distinct()
all_binary_paths_df = binary_df.select(F.col("path").alias("binary_file_path")).distinct()
unmatched_files_df = all_binary_paths_df.join(matched_paths_df, on="binary_file_path", how="left_anti")
zero_byte_df = matched_df.where(F.coalesce(F.col("file_size"), F.lit(0)) <= 0)

audit_rows = []
for row in matched_df.select("doc_id", "cleaned_url", "file_size", "binary_file_path").collect():
    status = "OK" if row.file_size and row.file_size > 0 else "ERROR"
    error_message = None if status == "OK" else f"ZERO_BYTE_OR_EMPTY_FILE path={row.binary_file_path}"
    audit_rows.append(audit_row(int(row.doc_id), row.cleaned_url, status, error_message, phase_started_at))

for row in missing_control_df.select("doc_id", "cleaned_url", "local_pdf_path").collect():
    audit_rows.append(
        audit_row(
            int(row.doc_id),
            row.cleaned_url,
            "ERROR",
            f"NO_MATCHING_BINARY_FILE local_pdf_path={row.local_pdf_path}",
            phase_started_at,
        )
    )

for failure in unreadable_failures:
    audit_rows.append(audit_row(None, None, "ERROR", f"UNREADABLE_FILE path={failure.file_path}: {failure.error_message}", phase_started_at))

append_audit_rows(audit_rows)

unmatched_files = [row.binary_file_path for row in unmatched_files_df.orderBy("binary_file_path").collect()]
zero_or_unreadable = [
    {"path": row.binary_file_path, "file_size": row.file_size}
    for row in zero_byte_df.select("binary_file_path", "file_size").orderBy("binary_file_path").collect()
]
zero_or_unreadable.extend(
    {"path": failure.file_path, "error_message": failure.error_message} for failure in unreadable_failures
)

summary = {
    "raw_pdf_folder": RAW_PDFS_DIR,
    "pdf_files_found_in_raw_pdfs": len(raw_pdf_paths),
    "available_control_table_rows": control_df.count(),
    "binary_files_successfully_read": binary_df.count(),
    "matched_pdfs": matched_df.count(),
    "unmatched_staged_pdfs": len(unmatched_files),
    "zero_byte_or_unreadable_pdfs": len(zero_or_unreadable),
    "unmatched_pdf_files": unmatched_files,
    "zero_byte_or_unreadable_pdf_files": zero_or_unreadable,
    "audit_rows_written": len(audit_rows),
    "pdfs_parsed": 0,
    "ai_parse_document_run": False,
    "ai_extract_run": False,
    "ai_json_validated": False,
    "final_result_rows_written": 0,
    "stopped_before_next_phase": True,
}

print("PHASE_READ_BINARY_SUMMARY=" + json.dumps(summary, sort_keys=True, default=str))

display(
    matched_df.select(
        "doc_id",
        "source_file_name",
        "input_status",
        "local_pdf_path",
        "binary_file_path",
        "file_size",
        "modificationTime",
    ).orderBy("doc_id")
)

display(unmatched_files_df.orderBy("binary_file_path"))
display(zero_byte_df.select("doc_id", "binary_file_path", "file_size").orderBy("doc_id"))
