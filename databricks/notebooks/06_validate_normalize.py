# Databricks notebook source
"""Validate and normalize Phase 8 raw AI extraction output.

This notebook consumes the Phase 8 raw extraction batch only. It prefers the
temporary view created by `05_extract_fixed_schema.py` when the same session is
used, otherwise it reads the persisted Phase 8 debug JSON output. It does not
run AI extraction, parse PDFs, download PDFs, or write final results.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


CATALOG_SCHEMA = "databricks_arrow_cata.main"
CONTROL_TABLE = f"{CATALOG_SCHEMA}.pdf_input_urls"
AUDIT_TABLE = f"{CATALOG_SCHEMA}.pdf_extraction_audit"

PHASE8_VIEW = "vw_pdf_ai_extract_phase8"
PHASE8_DEBUG_DIR = "/Volumes/databricks_arrow_cata/main/pdf_ai/debug/phase8_ai_extract_debug"
PHASE9_VIEW = "vw_pdf_validate_normalize_phase9"
PHASE9_DEBUG_DIR = "/Volumes/databricks_arrow_cata/main/pdf_ai/debug/phase9_validate_normalize_debug"

SCHEMA_VERSION = "v2"
STEP_VALIDATE_JSON = "VALIDATE_JSON"

SCALAR_FIELDS = [
    "document_title",
    "document_type",
    "part_number",
    "manufacturer",
    "description",
    "product_family",
    "product_category",
    "extraction_notes",
]
ARRAY_FIELDS = ["part_number_candidates", "manufacturer_candidates"]
MAP_FIELDS = [
    "electrical_specifications",
    "mechanical_specifications",
    "environmental_specifications",
    "compliance_specifications",
    "general_specifications",
]
FIXED_SCHEMA_FIELDS = SCALAR_FIELDS[:-1] + ARRAY_FIELDS + MAP_FIELDS + ["extraction_confidence", "extraction_notes"]
CORE_FIELDS = ["part_number", "manufacturer", "description"]


def utcnow() -> datetime:
    return datetime.utcnow()


def trim_to_none(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def parse_json_object(value: Any) -> tuple[Optional[dict[str, Any]], Optional[str]]:
    if value is None:
        return None, None
    if isinstance(value, dict):
        return value, None
    if not isinstance(value, str):
        return None, f"expected JSON object string, got {type(value).__name__}"
    text = value.strip()
    if not text:
        return None, None
    try:
        parsed = json.loads(text)
    except Exception as exc:
        return None, f"invalid JSON: {exc}"
    if not isinstance(parsed, dict):
        return None, f"expected JSON object, got {type(parsed).__name__}"
    return parsed, None


def unwrap_ai_value(value: Any) -> Any:
    if isinstance(value, dict) and "value" in value:
        return value.get("value")
    return value


def parse_possible_json(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    text = value.strip()
    if not text:
        return None
    if text[:1] not in ("[", "{"):
        return value
    try:
        return json.loads(text)
    except Exception:
        return value


def normalize_scalar(value: Any, field_name: str, errors: list[str]) -> Optional[str]:
    value = unwrap_ai_value(parse_possible_json(value))
    if value is None:
        return None
    if isinstance(value, (list, dict)):
        errors.append(f"{field_name} must be scalar")
        return None
    return trim_to_none(value)


def normalize_array(value: Any, field_name: str, errors: list[str]) -> Optional[list[str]]:
    value = unwrap_ai_value(parse_possible_json(value))
    if value is None:
        return None
    if not isinstance(value, list):
        errors.append(f"{field_name} must be an array")
        return None

    normalized = []
    for item in value:
        if isinstance(item, (dict, list)):
            item_text = json.dumps(item, sort_keys=True)
        else:
            item_text = trim_to_none(item)
        if item_text is not None:
            normalized.append(item_text)
    return normalized


def normalize_map(value: Any, field_name: str, errors: list[str]) -> Optional[dict[str, str]]:
    value = unwrap_ai_value(parse_possible_json(value))
    if value is None:
        return None
    if not isinstance(value, dict):
        errors.append(f"{field_name} must be a key-value object")
        return None

    normalized = {}
    for key, item in value.items():
        key_text = trim_to_none(key)
        if key_text is None:
            continue
        if isinstance(item, (dict, list)):
            item_text = json.dumps(item, sort_keys=True)
        else:
            item_text = trim_to_none(item)
        if item_text is not None:
            normalized[key_text] = item_text
    return normalized


def normalize_confidence(value: Any, errors: list[str]) -> Optional[float]:
    value = unwrap_ai_value(parse_possible_json(value))
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    try:
        return float(value)
    except Exception:
        errors.append("extraction_confidence must be numeric")
        return None


def raw_input_df():
    if spark.catalog.tableExists(PHASE8_VIEW):
        return spark.table(PHASE8_VIEW)
    return spark.read.format("json").load(PHASE8_DEBUG_DIR)


def response_payload(row_values: dict[str, Any]) -> tuple[dict[str, Any], Optional[str], dict[str, bool]]:
    raw_extraction_json = row_values.get("raw_extraction_json")
    parsed_raw, raw_error = parse_json_object(raw_extraction_json)
    if parsed_raw is None:
        return {}, raw_error, {}

    response = parsed_raw.get("response") if isinstance(parsed_raw.get("response"), dict) else parsed_raw
    response_error = parsed_raw.get("error_message") or parsed_raw.get("error")

    fields_present = {}
    fixed_schema_fields_present = row_values.get("fixed_schema_fields_present")
    parsed_presence, presence_error = parse_json_object(fixed_schema_fields_present)
    if parsed_presence is not None:
        fields_present = {field: bool(parsed_presence.get(field)) for field in FIXED_SCHEMA_FIELDS}
    elif presence_error is None:
        fields_present = {field: field in response for field in FIXED_SCHEMA_FIELDS}

    return response, response_error, fields_present


def source_value(row_values: dict[str, Any], response: dict[str, Any], field_name: str) -> Any:
    if field_name in response:
        return unwrap_ai_value(response.get(field_name))
    return row_values.get(field_name)


def normalized_row(row: Row) -> Row:
    started_at = utcnow()
    row_values = row.asDict(recursive=True)
    errors: list[str] = []

    response, response_error, fields_present = response_payload(row_values)
    raw_extraction_json = trim_to_none(row_values.get("raw_extraction_json"))
    attempt_status = trim_to_none(row_values.get("extraction_attempt_status"))
    attempt_error = trim_to_none(row_values.get("extraction_error")) or trim_to_none(response_error)

    if not raw_extraction_json:
        errors.append("raw_extraction_json is missing")
    if raw_extraction_json and response_error and not response and attempt_status != "ERROR":
        errors.append(response_error)
    if fields_present:
        missing_fields = [field for field in FIXED_SCHEMA_FIELDS if not fields_present.get(field)]
        if missing_fields:
            errors.append("missing fixed root fields: " + ", ".join(missing_fields))

    normalized_scalars = {
        field: normalize_scalar(source_value(row_values, response, field), field, errors) for field in SCALAR_FIELDS
    }
    normalized_arrays = {
        field: normalize_array(source_value(row_values, response, field), field, errors) for field in ARRAY_FIELDS
    }
    normalized_maps = {field: normalize_map(source_value(row_values, response, field), field, errors) for field in MAP_FIELDS}
    extraction_confidence = normalize_confidence(source_value(row_values, response, "extraction_confidence"), errors)

    if attempt_status == "ERROR" or (attempt_error and not raw_extraction_json):
        extraction_status = "FAILED"
    elif not raw_extraction_json:
        extraction_status = "FAILED"
    elif errors:
        extraction_status = "VALIDATION_FAILED"
    elif all(normalized_scalars.get(field) is not None for field in CORE_FIELDS):
        extraction_status = "SUCCESS"
    else:
        extraction_status = "PARTIAL_SUCCESS"

    error_parts = []
    if attempt_error:
        error_parts.append(attempt_error)
    error_parts.extend(errors)
    extraction_error = "; ".join(error_parts)[:2000] if error_parts else None
    finished_at = utcnow()

    return Row(
        doc_id=int(row_values["doc_id"]),
        source_url=row_values.get("source_url"),
        cleaned_url=row_values.get("cleaned_url"),
        source_file_name=row_values.get("source_file_name"),
        supplier_folder=row_values.get("supplier_folder"),
        url_part_hint=row_values.get("url_part_hint"),
        local_pdf_path=row_values.get("local_pdf_path"),
        document_title=normalized_scalars["document_title"],
        document_type=normalized_scalars["document_type"],
        part_number=normalized_scalars["part_number"],
        manufacturer=normalized_scalars["manufacturer"],
        description=normalized_scalars["description"],
        product_family=normalized_scalars["product_family"],
        product_category=normalized_scalars["product_category"],
        part_number_candidates=normalized_arrays["part_number_candidates"],
        manufacturer_candidates=normalized_arrays["manufacturer_candidates"],
        electrical_specifications=normalized_maps["electrical_specifications"],
        mechanical_specifications=normalized_maps["mechanical_specifications"],
        environmental_specifications=normalized_maps["environmental_specifications"],
        compliance_specifications=normalized_maps["compliance_specifications"],
        general_specifications=normalized_maps["general_specifications"],
        extraction_confidence=extraction_confidence,
        extraction_notes=normalized_scalars["extraction_notes"],
        raw_extraction_json=raw_extraction_json,
        schema_version=SCHEMA_VERSION,
        extraction_status=extraction_status,
        extraction_error=extraction_error,
        processed_at=finished_at,
        validation_started_at=started_at,
        validation_finished_at=finished_at,
        validation_duration_seconds=(finished_at - started_at).total_seconds(),
    )


def normalized_schema() -> StructType:
    return StructType(
        [
            StructField("doc_id", LongType(), False),
            StructField("source_url", StringType(), True),
            StructField("cleaned_url", StringType(), True),
            StructField("source_file_name", StringType(), True),
            StructField("supplier_folder", StringType(), True),
            StructField("url_part_hint", StringType(), True),
            StructField("local_pdf_path", StringType(), True),
            StructField("document_title", StringType(), True),
            StructField("document_type", StringType(), True),
            StructField("part_number", StringType(), True),
            StructField("manufacturer", StringType(), True),
            StructField("description", StringType(), True),
            StructField("product_family", StringType(), True),
            StructField("product_category", StringType(), True),
            StructField("part_number_candidates", ArrayType(StringType()), True),
            StructField("manufacturer_candidates", ArrayType(StringType()), True),
            StructField("electrical_specifications", MapType(StringType(), StringType()), True),
            StructField("mechanical_specifications", MapType(StringType(), StringType()), True),
            StructField("environmental_specifications", MapType(StringType(), StringType()), True),
            StructField("compliance_specifications", MapType(StringType(), StringType()), True),
            StructField("general_specifications", MapType(StringType(), StringType()), True),
            StructField("extraction_confidence", DoubleType(), True),
            StructField("extraction_notes", StringType(), True),
            StructField("raw_extraction_json", StringType(), True),
            StructField("schema_version", StringType(), False),
            StructField("extraction_status", StringType(), False),
            StructField("extraction_error", StringType(), True),
            StructField("processed_at", TimestampType(), False),
            StructField("validation_started_at", TimestampType(), False),
            StructField("validation_finished_at", TimestampType(), False),
            StructField("validation_duration_seconds", DoubleType(), False),
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


def write_validate_audit(normalized_rows: list[Row]) -> None:
    if not normalized_rows:
        return
    audit_rows = [
        Row(
            doc_id=row.doc_id,
            cleaned_url=row.cleaned_url,
            step_name=STEP_VALIDATE_JSON,
            status=row.extraction_status,
            error_message=row.extraction_error,
            retry_count=0,
            started_at=row.validation_started_at,
            finished_at=row.validation_finished_at,
            duration_seconds=row.validation_duration_seconds,
            model_endpoint=None,
            created_at=row.validation_finished_at,
        )
        for row in normalized_rows
    ]
    spark.createDataFrame(audit_rows, audit_schema()).write.mode("append").saveAsTable(AUDIT_TABLE)


raw_df = raw_input_df()
raw_row_count = raw_df.count()


def raw_column(column_name: str):
    if column_name in raw_df.columns:
        return F.col(f"r.{column_name}")
    return F.lit(None).cast(StringType())


raw_with_metadata_df = (
    raw_df.alias("r")
    .join(spark.table(CONTROL_TABLE).alias("u"), on="doc_id", how="left")
    .select(
        F.col("r.doc_id"),
        F.coalesce(F.col("u.source_url"), raw_column("source_url")).alias("source_url"),
        F.coalesce(F.col("u.cleaned_url"), raw_column("cleaned_url")).alias("cleaned_url"),
        F.coalesce(F.col("u.source_file_name"), raw_column("source_file_name")).alias("source_file_name"),
        F.coalesce(F.col("u.supplier_folder"), raw_column("supplier_folder")).alias("supplier_folder"),
        F.coalesce(F.col("u.url_part_hint"), raw_column("url_part_hint")).alias("url_part_hint"),
        F.coalesce(F.col("u.local_pdf_path"), raw_column("local_pdf_path")).alias("local_pdf_path"),
        *[F.col(f"r.{field}").alias(field) for field in FIXED_SCHEMA_FIELDS if field in raw_df.columns],
        raw_column("raw_extraction_json").alias("raw_extraction_json"),
        raw_column("extraction_attempt_status").alias("extraction_attempt_status"),
        raw_column("extraction_error").alias("extraction_error"),
        raw_column("fixed_schema_fields_present").alias("fixed_schema_fields_present"),
    )
    .dropDuplicates(["doc_id"])
    .orderBy("doc_id")
)

normalized_rows = [normalized_row(row) for row in raw_with_metadata_df.collect()]
if normalized_rows:
    normalized_df = spark.createDataFrame(normalized_rows, normalized_schema())
else:
    normalized_df = spark.createDataFrame([], normalized_schema())

normalized_df.createOrReplaceTempView(PHASE9_VIEW)
normalized_df.coalesce(1).write.mode("overwrite").json(PHASE9_DEBUG_DIR)
write_validate_audit(normalized_rows)

summary = {
    "raw_extraction_rows_used_as_input": raw_row_count,
    "normalized_rows_written_to_debug": normalized_df.count(),
    "temporary_view": PHASE9_VIEW,
    "debug_output_path": PHASE9_DEBUG_DIR,
    "validate_json_audit_rows_written": len(normalized_rows),
    "schema_version": SCHEMA_VERSION,
    "quality_gate_run": False,
    "phase10_started": False,
    "executed_inside_databricks": True,
}

print("PHASE9_VALIDATE_NORMALIZE_SUMMARY=" + json.dumps(summary, sort_keys=True, default=str))

display(
    normalized_df.groupBy("extraction_status")
    .count()
    .orderBy("extraction_status")
)

display(
    normalized_df.select(
        "doc_id",
        "source_file_name",
        "part_number",
        "manufacturer",
        "description",
        "extraction_confidence",
        "extraction_status",
        "extraction_error",
        "processed_at",
    ).orderBy("doc_id")
)
