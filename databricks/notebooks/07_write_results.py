# Databricks notebook source
"""Write normalized Phase 9 extraction rows into the final Delta table.

The write is retry-safe and idempotent by `doc_id` using MERGE. This notebook
does not run parsing, AI extraction, retry processing, or the quality gate.
"""

from __future__ import annotations

import json
from datetime import datetime

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


CATALOG_SCHEMA = "databricks_arrow_cata.main"
FINAL_RESULTS_TABLE = f"{CATALOG_SCHEMA}.datasheet_extraction_results"
AUDIT_TABLE = f"{CATALOG_SCHEMA}.pdf_extraction_audit"

PHASE9_VIEW = "vw_pdf_validate_normalize_phase9"
PHASE9_DEBUG_DIR = "/Volumes/databricks_arrow_cata/main/pdf_ai/debug/phase9_validate_normalize_debug"
WRITE_SOURCE_VIEW = "phase9_results_to_write"
STEP_WRITE_RESULT = "WRITE_RESULT"

RESULT_COLUMNS = [
    "doc_id",
    "source_url",
    "cleaned_url",
    "source_file_name",
    "supplier_folder",
    "url_part_hint",
    "local_pdf_path",
    "document_title",
    "document_type",
    "part_number",
    "manufacturer",
    "description",
    "product_family",
    "product_category",
    "part_number_candidates",
    "manufacturer_candidates",
    "electrical_specifications",
    "mechanical_specifications",
    "environmental_specifications",
    "compliance_specifications",
    "general_specifications",
    "extraction_confidence",
    "extraction_notes",
    "raw_extraction_json",
    "schema_version",
    "extraction_status",
    "extraction_error",
    "processed_at",
]


def utcnow() -> datetime:
    return datetime.utcnow()


def normalized_source_df():
    if spark.catalog.tableExists(PHASE9_VIEW):
        return spark.table(PHASE9_VIEW)
    return spark.read.format("json").load(PHASE9_DEBUG_DIR)


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


def write_result_audit(source_rows: list[Row], status: str, started_at: datetime, finished_at: datetime, error_message: str | None) -> None:
    if not source_rows:
        return
    audit_rows = [
        Row(
            doc_id=int(row.doc_id),
            cleaned_url=row.cleaned_url,
            step_name=STEP_WRITE_RESULT,
            status=status,
            error_message=error_message,
            retry_count=0,
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=(finished_at - started_at).total_seconds(),
            model_endpoint=None,
            created_at=finished_at,
        )
        for row in source_rows
    ]
    spark.createDataFrame(audit_rows, audit_schema()).write.mode("append").saveAsTable(AUDIT_TABLE)


source_df = normalized_source_df().select(*RESULT_COLUMNS).dropDuplicates(["doc_id"])
rows_to_write = source_df.select("doc_id", "cleaned_url").collect()
source_count = source_df.count()
source_df.createOrReplaceTempView(WRITE_SOURCE_VIEW)

merge_assignments = ",\n    ".join([f"target.{column} = source.{column}" for column in RESULT_COLUMNS if column != "doc_id"])
insert_columns = ", ".join(RESULT_COLUMNS)
insert_values = ", ".join([f"source.{column}" for column in RESULT_COLUMNS])

started_at = utcnow()
try:
    spark.sql(
        f"""
        MERGE INTO {FINAL_RESULTS_TABLE} AS target
        USING {WRITE_SOURCE_VIEW} AS source
        ON target.doc_id = source.doc_id
        WHEN MATCHED THEN UPDATE SET
            {merge_assignments}
        WHEN NOT MATCHED THEN INSERT ({insert_columns})
        VALUES ({insert_values})
        """
    )
    finished_at = utcnow()
    write_result_audit(rows_to_write, "OK", started_at, finished_at, None)
except Exception as exc:
    finished_at = utcnow()
    write_result_audit(rows_to_write, "ERROR", started_at, finished_at, str(exc)[:2000])
    raise

duplicate_doc_count = spark.sql(
    f"""
    SELECT COUNT(*) AS duplicate_doc_count
    FROM (
      SELECT doc_id
      FROM {FINAL_RESULTS_TABLE}
      GROUP BY doc_id
      HAVING COUNT(*) > 1
    )
    """
).collect()[0].duplicate_doc_count

summary = {
    "normalized_rows_used_as_input": source_count,
    "final_result_rows_total": spark.table(FINAL_RESULTS_TABLE).count(),
    "duplicate_doc_id_count": duplicate_doc_count,
    "write_result_audit_rows_written": len(rows_to_write),
    "write_mode": "MERGE_ON_DOC_ID",
    "quality_gate_run": False,
    "phase10_started": False,
    "executed_inside_databricks": True,
}

print("PHASE9_WRITE_RESULTS_SUMMARY=" + json.dumps(summary, sort_keys=True, default=str))

display(
    spark.table(FINAL_RESULTS_TABLE)
    .select(
        "doc_id",
        "source_file_name",
        "part_number",
        "manufacturer",
        "description",
        "extraction_confidence",
        "extraction_status",
        "extraction_error",
        "processed_at",
    )
    .orderBy("doc_id")
)
