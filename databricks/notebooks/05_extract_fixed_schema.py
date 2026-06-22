# Databricks notebook source
"""Run fixed-schema v2 AI extraction on already parsed PDF content.

This Phase 8 POC step uses Databricks `ai_extract` against
`pdf_parsed_documents.parsed_content` only. It preserves raw AI output for
inspection and writes AI_EXTRACT audit rows. It intentionally does not validate,
normalize, or write final `datasheet_extraction_results` rows.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

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
CONTROL_TABLE = f"{CATALOG_SCHEMA}.pdf_input_urls"
PARSED_TABLE = f"{CATALOG_SCHEMA}.pdf_parsed_documents"
AUDIT_TABLE = f"{CATALOG_SCHEMA}.pdf_extraction_audit"
FINAL_RESULTS_TABLE = f"{CATALOG_SCHEMA}.datasheet_extraction_results"
DEBUG_OUTPUT_DIR = "/Volumes/databricks_arrow_cata/main/pdf_ai/debug/phase8_ai_extract_debug"

PARSE_STATUS_PARSED = "PARSED"
STEP_AI_EXTRACT = "AI_EXTRACT"
SCHEMA_VERSION = "v2"
MODEL_ENDPOINT = "databricks-ai-functions.ai_extract"

FIXED_SCHEMA_FIELDS = [
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
]

EXTRACTION_SCHEMA = {
    "document_title": {"type": "string", "description": "Title printed in the PDF content."},
    "document_type": {"type": "string", "description": "Document type such as datasheet, specification sheet, manual, product page, connector specification, or SKU page."},
    "part_number": {"type": "string", "description": "Main part number only when clearly supported by PDF content."},
    "manufacturer": {"type": "string", "description": "Main manufacturer only when clearly supported by PDF content."},
    "description": {"type": "string", "description": "Product or part description supported by PDF content."},
    "product_family": {"type": "string", "description": "Product family supported by PDF content."},
    "product_category": {"type": "string", "description": "Product category supported by PDF content."},
    "part_number_candidates": {"type": "array", "items": {"type": "string"}, "description": "All possible part numbers found in the PDF content."},
    "manufacturer_candidates": {"type": "array", "items": {"type": "string"}, "description": "All possible manufacturers found in the PDF content."},
    "electrical_specifications": {"type": "string", "description": "Electrical specification key-value pairs as a JSON object string. Keep units in values."},
    "mechanical_specifications": {"type": "string", "description": "Mechanical specification key-value pairs as a JSON object string. Keep units in values."},
    "environmental_specifications": {"type": "string", "description": "Environmental specification key-value pairs as a JSON object string. Keep units in values."},
    "compliance_specifications": {"type": "string", "description": "Compliance, regulatory, or standards key-value pairs as a JSON object string. Keep units in values where applicable."},
    "general_specifications": {"type": "string", "description": "Other general specification key-value pairs as a JSON object string. Keep units in values."},
    "extraction_confidence": {"type": "number", "description": "Extractor confidence from 0.0 to 1.0 based only on PDF content support."},
    "extraction_notes": {"type": "string", "description": "Brief notes about uncertainty, missing fields, or candidate ambiguity."},
}

EXTRACTION_INSTRUCTIONS = """
Extract product/datasheet information from this PDF.
Use only values supported by the parsed PDF content.
Do not invent values.
If a scalar field is missing, return null.
If an array field is missing, return null or an empty array.
If a map field is missing, return null or an empty object.
If multiple possible part numbers exist, fill part_number_candidates.
If one part number is clearly the main one, fill part_number.
If multiple possible manufacturers exist, fill manufacturer_candidates.
If one manufacturer is clearly the main one, fill manufacturer.
Group specifications into electrical_specifications, mechanical_specifications,
environmental_specifications, compliance_specifications, and general_specifications.
Keep units inside values.
Keep URL-derived values separate from AI-confirmed extracted fields.
Do not use url_part_hint as confirmed part_number unless the parsed PDF content supports it.
Return the same schema every time.
""".strip()


def utcnow() -> datetime:
    return datetime.utcnow()


def configured_batch_size() -> int:
    configured_size = "5"
    if "dbutils" in globals():
        dbutils.widgets.text("batch_size", "5")
        configured_size = dbutils.widgets.get("batch_size")
    return max(1, min(int(configured_size), 5))


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def extraction_input_rows(limit_count: int) -> list[Row]:
    return (
        spark.table(PARSED_TABLE)
        .alias("p")
        .join(spark.table(CONTROL_TABLE).alias("u"), on="doc_id", how="inner")
        .where((F.col("p.parse_status") == PARSE_STATUS_PARSED) & F.col("p.parsed_content").isNotNull())
        .select(
            "doc_id",
            F.col("u.source_url"),
            F.col("p.cleaned_url"),
            F.col("u.source_file_name"),
            F.col("u.supplier_folder"),
            F.col("u.url_part_hint"),
            F.col("p.local_pdf_path"),
            F.col("p.parsed_content"),
        )
        .dropDuplicates(["doc_id"])
        .orderBy("doc_id")
        .limit(limit_count)
        .collect()
    )


def unwrap_ai_value(response: dict[str, Any], field_name: str) -> Any:
    value = response.get(field_name)
    if isinstance(value, dict) and "value" in value:
        return value.get("value")
    return value


def value_as_string(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True)
    return str(value)


def parse_raw_extraction(raw_extraction_json: Optional[str]) -> tuple[dict[str, Any], Optional[str]]:
    if not raw_extraction_json:
        return {}, None
    parsed = json.loads(raw_extraction_json)
    response = parsed.get("response") or {}
    error_message = parsed.get("error_message")
    if not isinstance(response, dict):
        response = {}
    return response, error_message


def raw_extract_for_row(row: Row) -> Row:
    started_at = utcnow()
    try:
        spark.createDataFrame([Row(parsed_content=row.parsed_content)]).createOrReplaceTempView("phase8_one_parsed_pdf")
        result = spark.sql(
            f"""
            SELECT CAST(ai_extract(
              parsed_content,
              {sql_literal(json.dumps(EXTRACTION_SCHEMA, separators=(",", ":")))},
              map('instructions', {sql_literal(EXTRACTION_INSTRUCTIONS)})
            ) AS STRING) AS raw_extraction_json
            FROM phase8_one_parsed_pdf
            """
        ).collect()[0]
        raw_extraction_json = result.raw_extraction_json
        response, error_message = parse_raw_extraction(raw_extraction_json)
        status = "ERROR" if error_message else "OK"
    except Exception as exc:
        raw_extraction_json = None
        response = {}
        error_message = str(exc)[:2000]
        status = "ERROR"

    finished_at = utcnow()
    values = {field_name: unwrap_ai_value(response, field_name) for field_name in FIXED_SCHEMA_FIELDS}
    return Row(
        doc_id=int(row.doc_id),
        source_url=row.source_url,
        cleaned_url=row.cleaned_url,
        source_file_name=row.source_file_name,
        supplier_folder=row.supplier_folder,
        url_part_hint=row.url_part_hint,
        local_pdf_path=row.local_pdf_path,
        raw_extraction_json=raw_extraction_json,
        schema_version=SCHEMA_VERSION,
        extraction_attempt_status=status,
        extraction_error=error_message,
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=(finished_at - started_at).total_seconds(),
        model_endpoint=MODEL_ENDPOINT,
        fixed_schema_fields_present=json.dumps({field: field in response for field in FIXED_SCHEMA_FIELDS}, sort_keys=True),
        document_title=value_as_string(values["document_title"]),
        document_type=value_as_string(values["document_type"]),
        part_number=value_as_string(values["part_number"]),
        manufacturer=value_as_string(values["manufacturer"]),
        description=value_as_string(values["description"]),
        product_family=value_as_string(values["product_family"]),
        product_category=value_as_string(values["product_category"]),
        part_number_candidates=value_as_string(values["part_number_candidates"]),
        manufacturer_candidates=value_as_string(values["manufacturer_candidates"]),
        electrical_specifications=value_as_string(values["electrical_specifications"]),
        mechanical_specifications=value_as_string(values["mechanical_specifications"]),
        environmental_specifications=value_as_string(values["environmental_specifications"]),
        compliance_specifications=value_as_string(values["compliance_specifications"]),
        general_specifications=value_as_string(values["general_specifications"]),
        extraction_confidence=value_as_string(values["extraction_confidence"]),
        extraction_notes=value_as_string(values["extraction_notes"]),
    )


def extraction_schema() -> StructType:
    return StructType(
        [
            StructField("doc_id", LongType(), False),
            StructField("source_url", StringType(), True),
            StructField("cleaned_url", StringType(), True),
            StructField("source_file_name", StringType(), True),
            StructField("supplier_folder", StringType(), True),
            StructField("url_part_hint", StringType(), True),
            StructField("local_pdf_path", StringType(), True),
            StructField("raw_extraction_json", StringType(), True),
            StructField("schema_version", StringType(), False),
            StructField("extraction_attempt_status", StringType(), False),
            StructField("extraction_error", StringType(), True),
            StructField("started_at", TimestampType(), False),
            StructField("finished_at", TimestampType(), False),
            StructField("duration_seconds", DoubleType(), False),
            StructField("model_endpoint", StringType(), False),
            StructField("fixed_schema_fields_present", StringType(), False),
            StructField("document_title", StringType(), True),
            StructField("document_type", StringType(), True),
            StructField("part_number", StringType(), True),
            StructField("manufacturer", StringType(), True),
            StructField("description", StringType(), True),
            StructField("product_family", StringType(), True),
            StructField("product_category", StringType(), True),
            StructField("part_number_candidates", StringType(), True),
            StructField("manufacturer_candidates", StringType(), True),
            StructField("electrical_specifications", StringType(), True),
            StructField("mechanical_specifications", StringType(), True),
            StructField("environmental_specifications", StringType(), True),
            StructField("compliance_specifications", StringType(), True),
            StructField("general_specifications", StringType(), True),
            StructField("extraction_confidence", StringType(), True),
            StructField("extraction_notes", StringType(), True),
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


def write_audit_rows(extraction_rows: list[Row]) -> None:
    if not extraction_rows:
        return
    audit_rows = [
        Row(
            doc_id=row.doc_id,
            cleaned_url=row.cleaned_url,
            step_name=STEP_AI_EXTRACT,
            status=row.extraction_attempt_status,
            error_message=row.extraction_error,
            retry_count=0,
            started_at=row.started_at,
            finished_at=row.finished_at,
            duration_seconds=row.duration_seconds,
            model_endpoint=row.model_endpoint,
            created_at=row.finished_at,
        )
        for row in extraction_rows
    ]
    spark.createDataFrame(audit_rows, audit_schema()).write.mode("append").saveAsTable(AUDIT_TABLE)


batch_size = configured_batch_size()
total_parsed_rows = spark.table(PARSED_TABLE).where(F.col("parse_status") == PARSE_STATUS_PARSED).count()
selected_input_rows = extraction_input_rows(batch_size)
extraction_rows = [raw_extract_for_row(row) for row in selected_input_rows]

if extraction_rows:
    extraction_df = spark.createDataFrame(extraction_rows, extraction_schema())
else:
    extraction_df = spark.createDataFrame([], extraction_schema())

extraction_df.createOrReplaceTempView("vw_pdf_ai_extract_phase8")
if extraction_rows:
    extraction_df.coalesce(1).write.mode("overwrite").json(DEBUG_OUTPUT_DIR)
write_audit_rows(extraction_rows)

summary = {
    "total_parsed_rows_available": total_parsed_rows,
    "parsed_documents_used_as_input": len(selected_input_rows),
    "successful_ai_extract_attempts": sum(1 for row in extraction_rows if row.extraction_attempt_status == "OK"),
    "failed_ai_extract_attempts": sum(1 for row in extraction_rows if row.extraction_attempt_status == "ERROR"),
    "raw_extraction_json_preserved_in_debug_path": DEBUG_OUTPUT_DIR if extraction_rows else None,
    "raw_extraction_json_displayed": True,
    "temporary_view": "vw_pdf_ai_extract_phase8",
    "ai_extract_run_inside_databricks": True,
    "ai_json_validated": False,
    "final_result_table_written": False,
    "quality_gate_run": False,
    "stopped_before_next_phase": True,
}

print("PHASE8_AI_EXTRACT_SUMMARY=" + json.dumps(summary, sort_keys=True, default=str))

display(
    extraction_df.select(
        "doc_id",
        "source_file_name",
        "raw_extraction_json",
        "fixed_schema_fields_present",
        "extraction_attempt_status",
        "extraction_error",
    )
    .orderBy("doc_id")
    .limit(3)
)

display(
    extraction_df.select(
        "doc_id",
        "source_file_name",
        "part_number",
        "manufacturer",
        "description",
        "extraction_confidence",
    )
    .orderBy("doc_id")
    .limit(10)
)

display(
    spark.table(AUDIT_TABLE)
    .where(F.col("step_name") == STEP_AI_EXTRACT)
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

print(
    "PHASE8_FINAL_RESULT_TABLE_ROW_COUNT_UNCHANGED_CHECK="
    + str(spark.table(FINAL_RESULTS_TABLE).count())
)
