# Databricks notebook source
"""Phase URL loader: pdf_input.txt -> pdf_input_urls.

Runs inside Databricks. This notebook intentionally stops at URL registration:
it does not download PDFs, read binaries, parse documents, run AI extraction, or
write final extraction results.
"""

from __future__ import annotations

import json
import re
import time
from datetime import datetime
from urllib.parse import unquote, urlparse

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


INPUT_URL_FILE = "/Volumes/databricks_arrow_cata/main/pdf_ai/input_urls/pdf_input.txt"
CONTROL_TABLE = "databricks_arrow_cata.main.pdf_input_urls"
AUDIT_TABLE = "databricks_arrow_cata.main.pdf_extraction_audit"

STATUS_NEW = "NEW"
STATUS_INVALID_URL = "INVALID_URL"
STEP_READ_INPUT_FILE = "READ_INPUT_FILE"
STEP_CLEAN_URLS = "CLEAN_URLS"
STEP_INSERT_CONTROL_TABLE = "INSERT_CONTROL_TABLE"

_HREF_RE = re.compile(r"href\s*=\s*(['\"])(.*?)\1", re.IGNORECASE)
_PLAIN_URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)
_TRAILING_INVALID_CHARS = " \t\r\n,;).(<>'\"}]"


def _utcnow():
    return datetime.utcnow()


def _extract_url_candidate(line):
    if line is None:
        return None
    href_match = _HREF_RE.search(line)
    if href_match:
        return href_match.group(2)
    plain_match = _PLAIN_URL_RE.search(line)
    if plain_match:
        return plain_match.group(0)
    return None


def _clean_url(url):
    if not url:
        return None
    cleaned = url.strip().rstrip(_TRAILING_INVALID_CHARS).strip()
    return cleaned or None


def _is_valid_pdf_url(url):
    if not url:
        return False
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return False
    path_lower = unquote(parsed.path or "").lower()
    host_lower = parsed.netloc.lower()
    return path_lower.endswith(".pdf") or ".pdf" in path_lower or "siliconexpert.com" in host_lower


def _derive_url_metadata(url):
    parsed = urlparse(url)
    path = unquote(parsed.path or "")
    parts = [part for part in path.split("/") if part]
    source_file_name = parts[-1] if parts else None

    supplier_folder = None
    for part in reversed(parts[:-1]):
        if part.endswith("_"):
            supplier_folder = part
            break

    url_part_hint = None
    if source_file_name:
        base = source_file_name
        if base.lower().endswith(".pdf"):
            base = base[:-4]
        if base.lower().startswith("skupage."):
            base = base.split(".")[-1]
        for suffix in ("datasheet", "spec"):
            if base.lower().endswith(suffix) and len(base) > len(suffix):
                base = base[: -len(suffix)]
                break
        url_part_hint = base or None

    return source_file_name, supplier_folder, url_part_hint


def _append_audit(step_name, status, started_at, error_message=None):
    finished_at = _utcnow()
    duration_seconds = (finished_at - started_at).total_seconds()
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
    row = [
        Row(
            doc_id=None,
            cleaned_url=None,
            step_name=step_name,
            status=status,
            error_message=error_message,
            retry_count=0,
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=duration_seconds,
            model_endpoint=None,
            created_at=finished_at,
        )
    ]
    try:
        spark.createDataFrame(row, audit_schema).write.mode("append").saveAsTable(AUDIT_TABLE)
        return True
    except Exception as exc:  # Audit should not block URL loading if the table is unavailable.
        print(f"AUDIT_NOT_WRITTEN step={step_name} error={exc}")
        return False


read_started = _utcnow()
lines = [row.value for row in spark.read.text(INPUT_URL_FILE).collect()]
_append_audit(STEP_READ_INPUT_FILE, "OK", read_started, f"rows_read={len(lines)}")

clean_started = _utcnow()
records = []
seen_cleaned_urls = set()
duplicate_input_rows = 0
valid_unique_rows = 0
invalid_rows = 0
url_candidates_extracted = 0

for row_number, raw_line in enumerate(lines, start=1):
    source_url = (raw_line or "").strip()
    if not source_url:
        continue

    candidate = _extract_url_candidate(source_url)
    cleaned_url = _clean_url(candidate)
    if candidate:
        url_candidates_extracted += 1

    if not _is_valid_pdf_url(cleaned_url):
        invalid_rows += 1
        records.append(
            {
                "row_number": row_number,
                "source_url": source_url,
                "cleaned_url": cleaned_url,
                "source_file_name": None,
                "supplier_folder": None,
                "url_part_hint": None,
                "input_status": STATUS_INVALID_URL,
                "local_pdf_path": None,
                "error_message": "INVALID_URL",
            }
        )
        continue

    if cleaned_url in seen_cleaned_urls:
        duplicate_input_rows += 1
        continue

    seen_cleaned_urls.add(cleaned_url)
    valid_unique_rows += 1
    source_file_name, supplier_folder, url_part_hint = _derive_url_metadata(cleaned_url)
    records.append(
        {
            "row_number": row_number,
            "source_url": source_url,
            "cleaned_url": cleaned_url,
            "source_file_name": source_file_name,
            "supplier_folder": supplier_folder,
            "url_part_hint": url_part_hint,
            "input_status": STATUS_NEW,
            "local_pdf_path": None,
            "error_message": None,
        }
    )

_append_audit(
    STEP_CLEAN_URLS,
    "OK",
    clean_started,
    json.dumps(
        {
            "url_candidates_extracted": url_candidates_extracted,
            "valid_cleaned_urls": valid_unique_rows,
            "duplicate_input_rows_skipped": duplicate_input_rows,
            "invalid_rows": invalid_rows,
        },
        sort_keys=True,
    ),
)

insert_started = _utcnow()
control_schema = StructType(
    [
        StructField("row_number", LongType(), False),
        StructField("source_url", StringType(), True),
        StructField("cleaned_url", StringType(), True),
        StructField("source_file_name", StringType(), True),
        StructField("supplier_folder", StringType(), True),
        StructField("url_part_hint", StringType(), True),
        StructField("input_status", StringType(), False),
        StructField("local_pdf_path", StringType(), True),
        StructField("error_message", StringType(), True),
    ]
)

candidate_df = spark.createDataFrame([Row(**record) for record in records], control_schema)
now_expr = F.current_timestamp()

valid_df = candidate_df.filter(F.col("input_status") == STATUS_NEW)
existing_valid_df = spark.table(CONTROL_TABLE).select("cleaned_url").where(F.col("cleaned_url").isNotNull()).distinct()
missing_valid_df = valid_df.join(existing_valid_df, on="cleaned_url", how="left_anti")
rows_inserted_valid = missing_valid_df.count()
already_existing_valid = valid_df.count() - rows_inserted_valid

valid_insert_df = missing_valid_df.select(
    "source_url",
    "cleaned_url",
    "source_file_name",
    "supplier_folder",
    "url_part_hint",
    "input_status",
    "local_pdf_path",
    "error_message",
).withColumn("created_at", now_expr).withColumn("updated_at", now_expr)

if rows_inserted_valid:
    valid_insert_df.write.mode("append").saveAsTable(CONTROL_TABLE)

invalid_df = candidate_df.filter(F.col("input_status") == STATUS_INVALID_URL)
existing_invalid_df = spark.table(CONTROL_TABLE).select("source_url", "input_status").where(
    F.col("input_status") == STATUS_INVALID_URL
)
missing_invalid_df = invalid_df.join(existing_invalid_df, on=["source_url", "input_status"], how="left_anti")
rows_inserted_invalid = missing_invalid_df.count()

invalid_insert_df = missing_invalid_df.select(
    "source_url",
    "cleaned_url",
    "source_file_name",
    "supplier_folder",
    "url_part_hint",
    "input_status",
    "local_pdf_path",
    "error_message",
).withColumn("created_at", now_expr).withColumn("updated_at", now_expr)

if rows_inserted_invalid:
    invalid_insert_df.write.mode("append").saveAsTable(CONTROL_TABLE)

rows_inserted_total = rows_inserted_valid + rows_inserted_invalid
_append_audit(
    STEP_INSERT_CONTROL_TABLE,
    "OK",
    insert_started,
    json.dumps(
        {
            "valid_rows_inserted": rows_inserted_valid,
            "invalid_rows_inserted": rows_inserted_invalid,
            "already_existing_valid_urls_skipped": already_existing_valid,
        },
        sort_keys=True,
    ),
)

status_counts = {
    row["input_status"]: row["count"]
    for row in spark.table(CONTROL_TABLE).groupBy("input_status").count().collect()
}
duplicate_cleaned_url_groups = spark.sql(
    f"""
    SELECT COUNT(*) AS duplicate_groups
    FROM (
      SELECT cleaned_url
      FROM {CONTROL_TABLE}
      WHERE cleaned_url IS NOT NULL AND input_status = 'NEW'
      GROUP BY cleaned_url
      HAVING COUNT(*) > 1
    )
    """
).collect()[0]["duplicate_groups"]

summary = {
    "input_file": INPUT_URL_FILE,
    "rows_read": len(lines),
    "urls_extracted": url_candidates_extracted,
    "valid_cleaned_urls": valid_unique_rows,
    "duplicate_urls_skipped": duplicate_input_rows + already_existing_valid,
    "duplicate_input_rows_skipped": duplicate_input_rows,
    "already_existing_valid_urls_skipped": already_existing_valid,
    "invalid_rows": invalid_rows,
    "rows_inserted_into_pdf_input_urls": rows_inserted_total,
    "valid_rows_inserted": rows_inserted_valid,
    "invalid_rows_inserted": rows_inserted_invalid,
    "duplicate_new_cleaned_url_groups": duplicate_cleaned_url_groups,
    "status_counts": status_counts,
    "pdfs_downloaded": 0,
    "pdfs_read_as_binary": 0,
    "pdfs_parsed": 0,
    "ai_extractions_run": 0,
}

print("PHASE_URL_LOADER_SUMMARY=" + json.dumps(summary, sort_keys=True))

display(
    spark.sql(
        f"""
        SELECT doc_id, source_url, cleaned_url, source_file_name, supplier_folder, url_part_hint, input_status
        FROM {CONTROL_TABLE}
        ORDER BY doc_id DESC
        LIMIT 20
        """
    )
)
