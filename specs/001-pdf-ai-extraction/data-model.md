# Phase 1 Data Model: Databricks PDF Fixed-Schema AI Extraction POC

All objects live under Unity Catalog `databricks_arrow_cata.main`. Column names, types, and status
enums are taken verbatim from PLAN.md (§7, §11, §12, §14, §16). DDL is in
[contracts/tables.sql](./contracts/tables.sql).

## Volume layout (`pdf_ai`)

| Path | Purpose |
|------|---------|
| `/Volumes/databricks_arrow_cata/main/pdf_ai/input_urls/pdf_input.txt` | Source-of-truth URL list (read-only input) |
| `/Volumes/databricks_arrow_cata/main/pdf_ai/raw_pdfs/` | Downloaded or manually-staged PDFs (never deleted) |
| `/Volumes/databricks_arrow_cata/main/pdf_ai/debug/` | Optional debug / failed-sample output |

## Entity 1 — `pdf_input_urls` (control table)

One row per cleaned PDF URL. `doc_id` is the stable processing key for the whole pipeline.

| Column | Type | Notes |
|--------|------|-------|
| `doc_id` | BIGINT GENERATED ALWAYS AS IDENTITY | Processing key |
| `source_url` | STRING | Original, preserved for traceability |
| `cleaned_url` | STRING | Normalized; dedup key |
| `source_file_name` | STRING | Basename from URL path |
| `supplier_folder` | STRING | Path segment ending in `_` (e.g. `ech_`) |
| `url_part_hint` | STRING | Weak filename-derived hint; **never** the confirmed part_number |
| `input_status` | STRING | See state machine below |
| `local_pdf_path` | STRING | Path in `raw_pdfs/` once acquired/staged |
| `error_message` | STRING | Latest error |
| `created_at` | TIMESTAMP | |
| `updated_at` | TIMESTAMP | |

**`input_status` state machine**:

```text
                ┌────────────────────────► SKIPPED_DUPLICATE   (exact cleaned_url already NEW)
(row inserted) ─┤
                ├────────────────────────► INVALID_URL         (unparseable line)
                └─► NEW ─┬─► DOWNLOADING ─┬─► DOWNLOADED        (Mode 1 success)
                         │                └─► DOWNLOAD_FAILED   (Mode 1 failure; retryable)
                         └─────────────────► MANUALLY_STAGED    (Mode 2: matched staged file)
```

- A `DOWNLOAD_FAILED` row may later become `MANUALLY_STAGED` (staged detection) or be retried.
- Validation rules: `cleaned_url` unique among non-duplicate rows; `source_url` always set;
  terminal-but-retryable = `DOWNLOAD_FAILED`.

## Entity 2 — `pdf_parsed_documents`

Parse outcome per document.

| Column | Type | Notes |
|--------|------|-------|
| `doc_id` | BIGINT | FK → `pdf_input_urls.doc_id` |
| `cleaned_url` | STRING | |
| `local_pdf_path` | STRING | |
| `parsed_content` | STRING or VARIANT | Parsed text/structure from `ai_parse_document` |
| `parse_status` | STRING | `PARSED` / `PARSE_FAILED` / `EMPTY_DOCUMENT` |
| `parse_error` | STRING | |
| `parsed_at` | TIMESTAMP | |

- `parse_status = EMPTY_DOCUMENT` when parser returns no extractable content; `PARSE_FAILED` when
  the parser errors. Neither aborts the batch.

## Entity 3 — `datasheet_extraction_results` (final, fixed schema v2)

One row per document. Metadata columns are carried from the control table; **fixed schema v2**
columns hold AI-confirmed content only; provenance columns hold raw output + status.

**Metadata / provenance carry-over**: `doc_id`, `source_url`, `cleaned_url`, `source_file_name`,
`supplier_folder`, `url_part_hint`, `local_pdf_path`.

**Fixed schema v2 (identical for every PDF)**:

| Column | Type |
|--------|------|
| `document_title` | STRING |
| `document_type` | STRING |
| `part_number` | STRING |
| `manufacturer` | STRING |
| `description` | STRING |
| `product_family` | STRING |
| `product_category` | STRING |
| `part_number_candidates` | ARRAY<STRING> |
| `manufacturer_candidates` | ARRAY<STRING> |
| `electrical_specifications` | MAP<STRING,STRING> |
| `mechanical_specifications` | MAP<STRING,STRING> |
| `environmental_specifications` | MAP<STRING,STRING> |
| `compliance_specifications` | MAP<STRING,STRING> |
| `general_specifications` | MAP<STRING,STRING> |
| `extraction_confidence` | DOUBLE |
| `extraction_notes` | STRING |

**Provenance / control**:

| Column | Type | Notes |
|--------|------|-------|
| `raw_extraction_json` | STRING | Raw model output, always preserved |
| `schema_version` | STRING | e.g. `v2` |
| `extraction_status` | STRING | `SUCCESS` / `PARTIAL_SUCCESS` / `VALIDATION_FAILED` / `FAILED` |
| `extraction_error` | STRING | |
| `processed_at` | TIMESTAMP | |

**Normalization & status rules** (PLAN.md §15):

- Empty strings → NULL; missing scalars → NULL; missing arrays → NULL or `[]`; missing maps → NULL
  or `{}`.
- `SUCCESS`: schema valid AND core fields extracted. `PARTIAL_SUCCESS`: schema valid but ≥1 core
  business field NULL. `VALIDATION_FAILED`: invalid structure/types (raw JSON still preserved).
  `FAILED`: extraction or parse failed.
- Core fields for status/quality-gate = `part_number`, `manufacturer`, `description`.
- Write path: `MERGE ... ON doc_id` (idempotent, retry-safe — no duplicate successes).

## Entity 4 — `pdf_extraction_audit`

One row per executed step per document.

| Column | Type |
|--------|------|
| `audit_id` | BIGINT GENERATED ALWAYS AS IDENTITY |
| `doc_id` | BIGINT |
| `cleaned_url` | STRING |
| `step_name` | STRING |
| `status` | STRING |
| `error_message` | STRING |
| `retry_count` | INT |
| `started_at` | TIMESTAMP |
| `finished_at` | TIMESTAMP |
| `duration_seconds` | DOUBLE |
| `model_endpoint` | STRING |
| `created_at` | TIMESTAMP |

`step_name` ∈ { `READ_INPUT_FILE`, `CLEAN_URLS`, `INSERT_CONTROL_TABLE`, `DOWNLOAD_PDF`,
`STAGE_PDF`, `READ_BINARY`, `PARSE_DOCUMENT`, `AI_EXTRACT`, `VALIDATE_JSON`, `WRITE_RESULT`,
`QUALITY_GATE` }. `model_endpoint` is set for `AI_EXTRACT` (and `PARSE_DOCUMENT` if applicable).

## Relationships

```text
pdf_input_urls (doc_id) 1──1 pdf_parsed_documents (doc_id)
pdf_input_urls (doc_id) 1──1 datasheet_extraction_results (doc_id)
pdf_input_urls (doc_id) 1──* pdf_extraction_audit (doc_id)   # many steps per doc
```

`doc_id` is the single join/processing key across all entities and the basis for idempotent
re-runs and retries.
