# Quickstart: Validate the PDF Fixed-Schema AI Extraction POC (in Databricks)

This is a **validation/run guide**, not implementation code. It proves the feature works end-to-end
**inside the Databricks workspace** on a small batch, per the constitution (Principle I, XII) and
PLAN.md §17–§20. Run each phase, then run its verification, and only proceed when it passes
(Principle XI).

## Prerequisites

- Databricks MCP connected via `.databrickscfg` profile `DEFAULT` (Principle II). Confirm with the
  MCP `get_current_user` / `list_compute` tools.
- Serverless compute on DBR **17.1+** (required for `ai_parse_document` — see research R1).
- Unity Catalog objects present (PLAN.md §1): catalog `databricks_arrow_cata`, schema `main`,
  Volume `pdf_ai`, with `input_urls/pdf_input.txt` and `raw_pdfs/` (PDFs may be manually staged).
- `.databrickscfg` is git-ignored; no secrets are committed (Principle III).

## Phase 1 — Confirm workspace, MCP, secret safety

```text
- MCP: get_current_user → confirms profile DEFAULT reaches host dbc-25b231f6-50…
- Shell: confirm `.databrickscfg` appears in .gitignore and `git status` does not list it.
```

**Pass when**: MCP returns the workspace user and `.databrickscfg` is not tracked by Git.

## Phase 2 — Confirm Unity Catalog paths

```text
- MCP get_volume_folder_details on input_urls/ and raw_pdfs/.
- execute_sql: SHOW SCHEMAS IN databricks_arrow_cata LIKE 'main'.
```

**Pass when**: `pdf_input.txt` exists, `raw_pdfs/` exists (note any pre-staged PDFs), schema `main`
exists.

## Phase 0 (setup) — Create tables

Run [contracts/tables.sql](./contracts/tables.sql) via MCP `execute_sql` (idempotent).

**Pass when**: `get_table_stats_and_schema` shows all four tables with the columns from
[data-model.md](./data-model.md).

## Phase 3 — Load URLs from `pdf_input.txt`

Run `databricks/notebooks/01_load_urls.py`. Reads the canonical file, extracts plain + anchor-tag
URLs, cleans/dedups, derives `source_file_name` / `supplier_folder` / `url_part_hint`, inserts into
`pdf_input_urls`, and writes `READ_INPUT_FILE`, `CLEAN_URLS`, `INSERT_CONTROL_TABLE` audit rows.

**Verify** (execute_sql):
```sql
SELECT input_status, COUNT(*) FROM databricks_arrow_cata.main.pdf_input_urls GROUP BY input_status;
SELECT COUNT(*) AS dup_new
FROM (SELECT cleaned_url FROM databricks_arrow_cata.main.pdf_input_urls
      WHERE input_status='NEW' GROUP BY cleaned_url HAVING COUNT(*)>1);
```
**Pass when**: one `NEW` row per unique cleaned URL, `dup_new = 0`, duplicates are
`SKIPPED_DUPLICATE`, bad lines are `INVALID_URL`, every `source_url` preserved (SC-001, SC-002).

## Phase 4 — Acquire PDFs (download OR detect staged) — start with 1

Run `databricks/notebooks/02_acquire_pdfs.py` limited to **1** `NEW` row. Mode 1 downloads with
deterministic name; on blocked egress mark `DOWNLOAD_FAILED`. Mode 2 detects staged files in
`raw_pdfs/` and marks `MANUALLY_STAGED`. Existing files are never deleted. Writes `DOWNLOAD_PDF` /
`STAGE_PDF` audit rows.

**Pass when**: the target row has `local_pdf_path` set and status `DOWNLOADED` or `MANUALLY_STAGED`;
no existing `raw_pdfs/` file was deleted; a download failure did not stop the run (SC-003, SC-007).

## Phase 5 — Read PDFs as binary

Run `databricks/notebooks/03_read_binary.py` (`binaryFile`), join to control rows on
`local_pdf_path`, write `READ_BINARY` audit rows.

**Pass when**: `path, content, length, modificationTime` obtained for the staged PDF and length > 0.

## Phase 6 — Parse documents

Run `databricks/notebooks/04_parse_documents.py` (`ai_parse_document(content, map('version','2.0'))`)
→ `pdf_parsed_documents`; write `PARSE_DOCUMENT` audit rows.

**Pass when**: at least one row has `parse_status = PARSED`; failures recorded as
`PARSE_FAILED`/`EMPTY_DOCUMENT` without aborting (SC-004).

## Phase 7 — Extract fixed schema v2

Run `databricks/notebooks/05_extract_fixed_schema.py` (`ai_query`, `responseFormat` json_object,
`failOnError => false`) per [contracts/extraction_schema_v2.md](./contracts/extraction_schema_v2.md).
Preserve `raw_extraction_json`; write `AI_EXTRACT` audit rows with `model_endpoint`.

**Pass when**: result carries every fixed-schema-v2 field (NULL/empty where unsupported) and
`raw_extraction_json` is non-empty; no value appears that isn't in the document (SC-004).

## Phase 8 — Validate & normalize

Run `databricks/notebooks/06_validate_normalize.py`: empty→NULL, validate arrays/maps/confidence,
assign `extraction_status`; write `VALIDATE_JSON` audit rows.

**Pass when**: every row has a valid `extraction_status`; `VALIDATION_FAILED` rows still retain raw
JSON (SC-005).

## Phase 9 — Write results (idempotent)

Run `databricks/notebooks/07_write_results.py` — `MERGE ... ON doc_id` into
`datasheet_extraction_results`; write `WRITE_RESULT` audit rows.

**Verify**: re-run the phase and confirm no duplicate `doc_id`:
```sql
SELECT doc_id, COUNT(*) c FROM databricks_arrow_cata.main.datasheet_extraction_results
GROUP BY doc_id HAVING c > 1;   -- expect 0 rows
```
**Pass when**: zero duplicates on re-run (SC-009).

## Phase 10 — Quality gate (before scaling)

Run `databricks/notebooks/08_quality_gate.py`:
```sql
SELECT
  SUM(extraction_status='SUCCESS')          AS success,
  SUM(extraction_status='PARTIAL_SUCCESS')  AS partial,
  SUM(extraction_status='FAILED')           AS failed,
  SUM(part_number  IS NULL)                 AS pn_null,
  SUM(manufacturer IS NULL)                 AS mfr_null,
  SUM(description  IS NULL)                  AS desc_null,
  SUM(part_number IS NULL AND manufacturer IS NULL AND description IS NULL) AS all_core_null
FROM databricks_arrow_cata.main.datasheet_extraction_results;
```
Writes a `QUALITY_GATE` audit row (PASS/STOP) (SC-006, SC-008).

**Pass when**: counts are produced and a stop/go signal emitted. If `all_core_null` exceeds half the
batch → **STOP**, improve schema/instructions, do not process all URLs.

## Scale-up

Only after the gate PASSes on 1 PDF: repeat Phases 4–10 for **up to 5** PDFs, re-run the gate, then
process the full `pdf_input.txt` list. Use the retry selection (`DOWNLOAD_FAILED` /
`PARSE_FAILED` / `extraction_status IN (FAILED, VALIDATION_FAILED)`) keyed by `doc_id` to recover
failures without duplicating successes (SC-009, SC-010).

## End-to-end definition of done

The POC is complete when, inside the workspace, a small batch flows
`pdf_input.txt → pdf_input_urls → raw_pdfs PDFs → pdf_parsed_documents → fixed-schema results →
pdf_extraction_audit`, with audit coverage for every step and the quality gate run before full
processing (SC-010, PLAN.md §20).
