"""Shared helpers for the Databricks PDF extraction POC.

These helpers intentionally avoid local filesystem assumptions. They are mirrored
for Databricks notebook use and keep PLAN.md URL rules in one place.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Optional
from urllib.parse import unquote, urlparse


CATALOG = "databricks_arrow_cata"
SCHEMA = "main"
VOLUME = "pdf_ai"

INPUT_URL_FILE = "/Volumes/databricks_arrow_cata/main/pdf_ai/input_urls/pdf_input.txt"
RAW_PDFS_DIR = "/Volumes/databricks_arrow_cata/main/pdf_ai/raw_pdfs/"

PDF_INPUT_URLS_TABLE = "databricks_arrow_cata.main.pdf_input_urls"
PDF_EXTRACTION_AUDIT_TABLE = "databricks_arrow_cata.main.pdf_extraction_audit"

STATUS_NEW = "NEW"
STATUS_SKIPPED_DUPLICATE = "SKIPPED_DUPLICATE"
STATUS_INVALID_URL = "INVALID_URL"
STATUS_DOWNLOADING = "DOWNLOADING"
STATUS_DOWNLOADED = "DOWNLOADED"
STATUS_DOWNLOAD_FAILED = "DOWNLOAD_FAILED"
STATUS_MANUALLY_STAGED = "MANUALLY_STAGED"

STEP_READ_INPUT_FILE = "READ_INPUT_FILE"
STEP_CLEAN_URLS = "CLEAN_URLS"
STEP_INSERT_CONTROL_TABLE = "INSERT_CONTROL_TABLE"
STEP_DOWNLOAD_PDF = "DOWNLOAD_PDF"
STEP_STAGE_PDF = "STAGE_PDF"
STEP_READ_BINARY = "READ_BINARY"
STEP_PARSE_DOCUMENT = "PARSE_DOCUMENT"
STEP_AI_EXTRACT = "AI_EXTRACT"
STEP_VALIDATE_JSON = "VALIDATE_JSON"
STEP_WRITE_RESULT = "WRITE_RESULT"
STEP_QUALITY_GATE = "QUALITY_GATE"

_HREF_RE = re.compile(r"href\s*=\s*(['\"])(.*?)\1", re.IGNORECASE)
_PLAIN_URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)
_TRAILING_INVALID_CHARS = " \t\r\n,;).(<>'\"}]"


@dataclass(frozen=True)
class UrlMetadata:
    source_file_name: Optional[str]
    supplier_folder: Optional[str]
    url_part_hint: Optional[str]


def extract_url_candidate(line: str) -> Optional[str]:
    """Extract a URL from either an HTML href row or a plain text row."""
    if line is None:
        return None

    href_match = _HREF_RE.search(line)
    if href_match:
        return href_match.group(2)

    plain_match = _PLAIN_URL_RE.search(line)
    if plain_match:
        return plain_match.group(0)

    return None


def clean_url(url: Optional[str]) -> Optional[str]:
    """Trim a URL and remove trailing invalid punctuation required by PLAN.md."""
    if not url:
        return None
    cleaned = url.strip().rstrip(_TRAILING_INVALID_CHARS).strip()
    return cleaned or None


def is_valid_pdf_url(url: Optional[str]) -> bool:
    """Validate http(s) URLs that are PDFs or SiliconExpert PDF-like URLs."""
    if not url:
        return False

    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return False

    path_lower = unquote(parsed.path or "").lower()
    host_lower = parsed.netloc.lower()
    return path_lower.endswith(".pdf") or ".pdf" in path_lower or "siliconexpert.com" in host_lower


def derive_url_metadata(url: str) -> UrlMetadata:
    """Derive filename, supplier folder, and weak part hint from a cleaned URL."""
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

    return UrlMetadata(source_file_name, supplier_folder, url_part_hint)


def deterministic_pdf_name(doc_id: int, cleaned_url: str, source_file_name: str) -> str:
    """Build PLAN.md's deterministic raw PDF filename."""
    url_hash = hashlib.sha256(cleaned_url.encode("utf-8")).hexdigest()[:16]
    return f"{doc_id}_{url_hash}_{source_file_name}"
