# Phase 0 Research: Databricks PDF Fixed-Schema AI Extraction POC

All decisions are constrained by PLAN.md (source of truth) and the project constitution v1.0.0.
No paths, tables, or scope are introduced beyond PLAN.md.

## R1. PDF parsing function

- **Decision**: Use `ai_parse_document(content, map('version', '2.0'))` on the `binaryFile`
  `content` column, exactly as named in PLAN.md §11. Store parsed text/output and a parse status in
  `pdf_parsed_documents`.
- **Rationale**: PLAN.md mandates the Databricks-native parser; it is the task-specific function for
  PDF/document parsing and avoids managing any external endpoint (Principle I, II, V). The parsed
  result exposes pages/elements (`parsed:pages[*].elements[*].content`) and an error field
  (`parsed:error`) that lets us classify `PARSED` / `EMPTY_DOCUMENT` / `PARSE_FAILED` without
  aborting the batch (Principle VII, IX).
- **Alternatives considered**: Third-party Python PDF libraries (pypdf, pdfplumber) — rejected:
  would run extraction logic off-platform or require extra deps, weakening Databricks-first
  guarantees and the native parsing requirement.
- **Constraint noted**: `ai_parse_document` requires DBR **17.1+**; serverless SQL/compute in the
  free workspace must be confirmed at Phase 2 (workspace verification). If `version 2.0` is rejected
  by the runtime, fall back to the default-signature call and record the variance in the audit row —
  still native, still PLAN-compliant.

## R2. Fixed-schema-v2 extraction function

- **Decision**: Use `ai_query` with `responseFormat => '{"type":"json_object"}'` (structured/JSON
  output) and `failOnError => false`, then parse the JSON string into fixed schema v2 with
  `from_json`. Preserve the raw model response string in `raw_extraction_json`.
- **Rationale**: Fixed schema v2 contains **nested arrays** (`part_number_candidates`,
  `manufacturer_candidates`) and **five `MAP<STRING,STRING>` specification groups**. `ai_extract`
  returns only a flat struct of named entities and cannot produce grouped maps or candidate arrays,
  so this is precisely the "complex nested JSON" case the skill designates for `ai_query`.
  `failOnError => false` returns a STRUCT with `.response` and `.error`, so one bad PDF yields a
  `FAILED`/`VALIDATION_FAILED` row instead of crashing the batch (Principle VII, X).
- **Alternatives considered**:
  - `ai_extract(text, ARRAY(...))` — rejected: flat-only, no maps/candidate arrays, no confidence.
  - Multiple `ai_*` calls merged — rejected: more model calls = more free-tier cost (Principle IV)
    and no single raw JSON to preserve.
- **No-invented-values control**: The prompt explicitly instructs "use only values supported by the
  document; if a field is missing return null; do not invent part numbers, manufacturers,
  descriptions, or specifications; URL/filename values are hints only" (Principle VIII, PLAN.md §13).

## R3. Reading PDFs as binary from the Volume

- **Decision**: Read with `spark.read.format("binaryFile").load("/Volumes/databricks_arrow_cata/
  main/pdf_ai/raw_pdfs/")` (equivalently `read_files(..., format => 'binaryFile')`), yielding
  `path, content, length, modificationTime`. Join back to `pdf_input_urls` on `local_pdf_path`.
- **Rationale**: Matches PLAN.md §10 expected columns; native, no download to driver, content stays
  in the Volume (Principle V). Path-based join supports both acquisition modes uniformly.
- **Alternatives considered**: `dbutils.fs`/Python file reads — rejected: less scalable and bypasses
  the Spran/Delta-native path the rest of the pipeline uses.

## R4. Two acquisition modes (download vs manual/staged)

- **Decision**: Phase 4 attempts download for `NEW` rows using deterministic naming
  `{doc_id}_{url_hash}_{source_file_name}` (PLAN.md §9). On success → `DOWNLOADED`; on failure →
  `DOWNLOAD_FAILED` with error, batch continues. Independently, a staged-detection pass lists
  existing `raw_pdfs/` files and matches them to control rows by deterministic name →
  `source_file_name` → `url_part_hint` → other filename metadata, marking matches `MANUALLY_STAGED`.
  Unmatched staged files are preserved as staged records and processed with available metadata.
- **Rationale**: Free Databricks may block outbound access; manual/staged mode is first-class, not an
  error path (PLAN.md, Principle IV). Existing files are never deleted (PLAN.md §9). Per-row status
  isolates failures (Principle VII).
- **Alternatives considered**: Fail-fast on blocked egress — rejected outright by the constitution
  and PLAN.md.

## R5. Idempotent, retry-safe writes

- **Decision**: Write `datasheet_extraction_results` via `MERGE INTO ... ON target.doc_id =
  source.doc_id WHEN MATCHED THEN UPDATE WHEN NOT MATCHED THEN INSERT`. Retry selects only
  `input_status = DOWNLOAD_FAILED`, `parse_status = PARSE_FAILED`, or `extraction_status IN
  (FAILED, VALIDATION_FAILED)`, increments `retry_count`, keeps the latest error.
- **Rationale**: `doc_id` is the stable processing key (PLAN.md §18); MERGE prevents duplicate
  successful results and makes the whole batch re-runnable (Principle X). Successful rows are not
  reprocessed, saving free-tier quota (Principle IV).
- **Alternatives considered**: delete-then-insert by `doc_id` — acceptable equivalent per PLAN.md;
  MERGE chosen for atomicity.

## R6. Audit-first logging

- **Decision**: A shared audit writer appends one row to `pdf_extraction_audit` per executed step
  for each `doc_id`, capturing `step_name`, `status`, `error_message`, `retry_count`, `started_at`,
  `finished_at`, `duration_seconds`, and `model_endpoint` (for `AI_EXTRACT`). Step names are exactly
  the 11 in PLAN.md §16 + the spec (`READ_INPUT_FILE`, `CLEAN_URLS`, `INSERT_CONTROL_TABLE`,
  `DOWNLOAD_PDF`, `STAGE_PDF`, `READ_BINARY`, `PARSE_DOCUMENT`, `AI_EXTRACT`, `VALIDATE_JSON`,
  `WRITE_RESULT`, `QUALITY_GATE`).
- **Rationale**: Principle IX requires status+error for every major step; centralizing the writer
  guarantees uniform coverage and timing.
- **Alternatives considered**: Logging only failures — rejected: audit-first requires success rows
  too, for traceability and the quality gate.

## R7. Quality gate before full run

- **Decision**: Phase 10 runs after the 1-PDF and ≤5-PDF batches: counts of `SUCCESS`,
  `PARTIAL_SUCCESS`, `FAILED`, and counts where `part_number`, `manufacturer`, `description`, and all
  three core fields are NULL. If "most fields are NULL", emit a STOP signal and do not process the
  full URL list until schema/instructions improve.
- **Rationale**: PLAN.md §17 + Principle IV — protect quota, prove schema strength before scale.
  "Core extracted fields" = `part_number`, `manufacturer`, `description` (PLAN.md §17 checks).
- **Threshold**: Interpreted as: if the count where all three core fields are NULL exceeds half the
  processed batch, STOP. This is a reviewable POC heuristic, surfaced in the gate output, not a
  silent decision.

## R8. URL cleaning & metadata derivation

- **Decision**: Reusable helper extracts `href` from HTML anchor lines, accepts plain `http(s)`
  URLs, strips trailing `, ; ) ( ` and whitespace, deduplicates exact cleaned URLs, and derives
  `source_file_name` (URL path basename), `supplier_folder` (path segment ending in `_`), and
  `url_part_hint` (weak candidate from filename, e.g. `skupage.242307.pdf → 242307`,
  `ca3102e183pf80spec.pdf → ca3102e183pf80`). First occurrence → `NEW`; later exact duplicate →
  `SKIPPED_DUPLICATE`; unparseable → `INVALID_URL`. `source_url` always preserved.
- **Rationale**: Directly encodes PLAN.md §8 rules; `url_part_hint` stays metadata-only and is never
  promoted to `part_number` unless PDF content confirms it (Principle VIII).
- **Alternatives considered**: Full HTML parser dependency — rejected as overkill; targeted `href`
  extraction + normalization suffices for the input format shown in PLAN.md.

## Open items for Phase 2 verification (not blockers)

These are confirmed in-workspace during Phase 2 (workspace/UC verification), not design unknowns:

1. Serverless runtime is DBR 17.1+ so `ai_parse_document` is available (R1).
2. `ai_query` model endpoint name available in the free workspace (e.g.
   `databricks-claude-sonnet-4` or the workspace default) — recorded in `model_endpoint`.
3. Whether outbound egress to PDF hosts is permitted (decides whether Mode 1 download is exercised
   or the run relies on Mode 2 staged PDFs). Either way the pipeline proceeds.
