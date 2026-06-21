---
description: "Task list for Databricks PDF URL Looping & Fixed-Schema AI Extraction POC"
---

# Tasks: Databricks PDF URL Looping & Fixed-Schema AI Extraction POC

**Input**: Design documents from `specs/001-pdf-ai-extraction/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/ (tables.sql,
extraction_schema_v2.md, audit_contract.md), quickstart.md

**Source of truth**: `PLAN.md`. No paths, tables, schemas, statuses, or scope beyond PLAN.md.

**Tests**: No automated test suite. Per the constitution (Principle I, XI), every phase is validated
**inside the Databricks workspace via MCP** with a validation query/check before proceeding. These
in-workspace checks are the acceptance gate — not local pytest.

## Execution rules (apply to every phase)

- **Do not implement now.** Each phase is implemented later via an isolated `/speckit-implement` run.
- Every real pipeline step **executes inside Databricks** through the MCP server; the local repo only
  holds the mirrored notebook/SQL assets for version control.
- MCP auth comes from `.databrickscfg` profile `DEFAULT`. Never commit `.databrickscfg`, tokens, or
  secrets (only `.databrickscfg.example`).
- Start with **1 PDF**, then **≤5**, then full — only after the quality gate (US5) passes.
- Never stop the batch on one bad URL/PDF; never delete existing `raw_pdfs/` files; key all
  processing and idempotent writes on `doc_id`.
- Every phase writes/append the relevant audit rows (`pdf_extraction_audit`) per
  [contracts/audit_contract.md](./contracts/audit_contract.md).

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependency on incomplete tasks)
- **[Story]**: US1–US6 maps to the spec's user stories; Setup/Foundational/Polish have no story label

## Path Conventions

Repo-mirrored Databricks assets (per plan.md Structure Decision):

```text
databricks/
├── sql/
│   ├── 00_create_tables.sql
│   └── checks/                 # in-workspace validation queries, one per phase
├── notebooks/                  # 01–09, one per pipeline phase
└── lib/
    └── pipeline_common.py      # url cleaning, deterministic naming, audit writer, status constants
.databrickscfg.example
```

---

## Phase 1: Setup — Workspace, MCP & Unity Catalog validation

**Purpose**: Confirm secret safety, MCP/workspace access, and that all Unity Catalog paths exist
(PLAN.md Phases 1–2; constitution Principles I, II, III, V).

**Independent test**: MCP runs a command inside the workspace, `pdf_input.txt` is readable,
`raw_pdfs/` exists with any staged PDFs listed, and no secret file is tracked by Git.

- [X] T001 Create repo scaffolding directories `databricks/sql/`, `databricks/sql/checks/`, `databricks/notebooks/`, `databricks/lib/` at repository root
- [X] T002 [P] Create `.databrickscfg.example` at repository root with placeholder-only values (no real host/token); document the `DEFAULT` profile shape
- [X] T003 [P] Add/confirm `.gitignore` entries for `.databrickscfg`, `*.token`, and secret files; verify `git status` does NOT list `.databrickscfg` (PLAN.md §4; Principle III)
- [X] T004 Verify `.databrickscfg` exists locally and profile `DEFAULT` is present (local check only; do not print token contents)
- [X] T005 Verify MCP connects to the Databricks workspace via MCP `get_current_user` / `list_compute`; confirm it reaches host `dbc-25b231f6-50…`
- [X] T006 Execute a simple in-workspace command via MCP `execute_sql` (e.g. `SELECT current_catalog(), current_user()`) and confirm it ran inside Databricks, not locally
- [X] T007 [P] Validate catalog `databricks_arrow_cata` and schema `databricks_arrow_cata.main` exist via MCP `execute_sql` (`SHOW SCHEMAS IN databricks_arrow_cata LIKE 'main'`)
- [X] T008 [P] Validate Volume `databricks_arrow_cata.main.pdf_ai` and that input file `/Volumes/databricks_arrow_cata/main/pdf_ai/input_urls/pdf_input.txt` is readable, via MCP `get_volume_folder_details` / volume file tools
- [X] T009 [P] Validate raw PDF folder `/Volumes/databricks_arrow_cata/main/pdf_ai/raw_pdfs/` exists and **list manually staged PDFs** already uploaded there (capture filenames for US2 matching)
- [X] T010 [P] Validate optional debug folder `/Volumes/databricks_arrow_cata/main/pdf_ai/debug/`; create it if missing (never touch `raw_pdfs/` contents)
- [ ] T011 Record Phase 1 validation results (catalog/schema/volume/paths/staged-file list, runtime version for `ai_parse_document` DBR 17.1+, and the `ai_query` model endpoint name) in `databricks/sql/checks/01_setup_checks.sql` as commented evidence

**Checkpoint**: Workspace reachable via MCP, all UC paths confirmed, secrets safe. Stop & validate.

---

## Phase 2: Foundational — Delta tables & shared library (BLOCKS all user stories)

**Purpose**: Create the four Delta tables and the shared helper library every downstream phase
depends on (PLAN.md Phase 3, §6–§7, §11, §14, §16; constitution Principles V, VII, IX).

**⚠️ No user story can start until this phase is complete.**

- [ ] T012 Create `databricks/sql/00_create_tables.sql` mirroring [contracts/tables.sql](./contracts/tables.sql) — idempotent `CREATE TABLE IF NOT EXISTS` for `pdf_input_urls`, `pdf_parsed_documents`, `datasheet_extraction_results`, `pdf_extraction_audit` with exact PLAN.md columns/types; do NOT drop or delete existing data
- [ ] T013 Execute `databricks/sql/00_create_tables.sql` via MCP `execute_sql` to create/validate all four tables in `databricks_arrow_cata.main`
- [ ] T014 Create `databricks/sql/checks/03_tables_check.sql` and run it via MCP: confirm all four tables exist, are queryable, and column names/types match PLAN.md (use `get_table_stats_and_schema`)
- [ ] T015 Create `databricks/lib/pipeline_common.py` with shared helpers: URL extraction/cleaning (href + plain, strip trailing `, ; ) (` + whitespace), exact-dedup, metadata derivation (`source_file_name`, `supplier_folder` ending in `_`, `url_part_hint`), deterministic name builder `{doc_id}_{url_hash}_{source_file_name}`, status/step constants, and an `audit_writer` that appends rows to `pdf_extraction_audit` per [contracts/audit_contract.md](./contracts/audit_contract.md)

**Checkpoint**: Tables exist & match schema; shared helpers + audit writer available. Stop & validate.

---

## Phase 3 (US1, P1): Load & register PDF URLs from `pdf_input.txt`

**Goal**: Read the canonical input file, extract plain + anchor-tag URLs, clean/dedup, derive
metadata, and register one control-table row per cleaned URL (PLAN.md Phase 4, §8).

**Independent test**: `pdf_input_urls` has one `NEW` row per unique cleaned URL, duplicates marked
`SKIPPED_DUPLICATE`, bad lines `INVALID_URL`, `source_url` preserved, metadata populated.

- [ ] T016 [US1] Create notebook `databricks/notebooks/01_load_urls.py` that reads the full contents of `/Volumes/databricks_arrow_cata/main/pdf_ai/input_urls/pdf_input.txt` (canonical path only — never `pdf_urls.txt`); write `READ_INPUT_FILE` audit row
- [ ] T017 [US1] In `01_load_urls.py`, extract URLs from plain `http(s)` lines AND from HTML anchor tags via `href`, then clean/normalize (strip trailing `, ; ) (` and whitespace) using `databricks/lib/pipeline_common.py`; write `CLEAN_URLS` audit row
- [ ] T018 [US1] In `01_load_urls.py`, validate URL format, deduplicate exact cleaned URLs (first → `NEW`, later exact dup → `SKIPPED_DUPLICATE`), mark unparseable lines `INVALID_URL`, and derive `source_file_name` / `supplier_folder` / `url_part_hint` (hint stays metadata-only — never promoted to `part_number`)
- [ ] T019 [US1] In `01_load_urls.py`, insert rows into `databricks_arrow_cata.main.pdf_input_urls` preserving `source_url`, setting `created_at`/`updated_at`; guard against duplicate control-table inserts on re-run (idempotent by `cleaned_url`); write `INSERT_CONTROL_TABLE` audit row
- [ ] T020 [US1] Create `databricks/sql/checks/04_urls_check.sql` and run via MCP: status counts by `input_status`, zero duplicate `NEW` `cleaned_url`, and a sample showing populated metadata (SC-001, SC-002)

**Checkpoint**: Control table populated & deduplicated. Stop & validate before US2.

---

## Phase 4 (US2, P1): Acquire PDFs — automatic download OR manual/staged matching

**Goal**: For a small batch (1 first, then ≤5) make each PDF available in `raw_pdfs/` by download
(Mode 1) or by detecting/matching manually staged files (Mode 2), without aborting on failures
(PLAN.md Phase 5, §9).

**Independent test**: At least one row reaches `DOWNLOADED` or `MANUALLY_STAGED` with `local_pdf_path`
set; download failures logged as `DOWNLOAD_FAILED`; no existing `raw_pdfs/` file deleted.

- [ ] T021 [US2] Create notebook `databricks/notebooks/02_acquire_pdfs.py` that selects `input_status = NEW` rows **limited to 1** for the first batch (parameterized batch size, default 1, max 5)
- [ ] T022 [US2] In `02_acquire_pdfs.py`, Mode 1: attempt download, save to `raw_pdfs/` with deterministic name `{doc_id}_{url_hash}_{source_file_name}`, set `local_pdf_path`, transition `NEW → DOWNLOADING → DOWNLOADED`; on failure set `DOWNLOAD_FAILED` + `error_message` and continue (never stop batch); write `DOWNLOAD_PDF` audit rows
- [ ] T023 [US2] In `02_acquire_pdfs.py`, Mode 2: detect PDFs already in `raw_pdfs/` and match to control rows by deterministic name → `source_file_name` → `url_part_hint` → available filename metadata; set matched rows `MANUALLY_STAGED` with `local_pdf_path`; preserve unmatched staged files as staged records processed with available metadata; never delete existing files; write `STAGE_PDF` audit rows
- [ ] T024 [US2] Create `databricks/sql/checks/05_acquire_check.sql` and run via MCP: confirm ≥1 row has `local_pdf_path` and status `DOWNLOADED`/`MANUALLY_STAGED`, list `DOWNLOAD_FAILED` rows with errors, and verify staged-file count in `raw_pdfs/` is unchanged or higher (never lower) (SC-003, SC-007)

**Checkpoint**: ≥1 PDF available via either mode; failures logged. Stop & validate before US3.

---

## Phase 5 (US3, P1): Read binary → parse → AI fixed-schema extraction

**Goal**: Read available PDFs as binary, parse with `ai_parse_document`, and extract fixed schema v2
with `ai_query` — one PDF first. Same schema for every PDF; no invented values; raw output preserved
(PLAN.md Phases 6–8, §10–§13; constitution Principles VII, VIII).

**Independent test**: ≥1 PDF read as binary, parsed (`PARSED`), and extracted into all fixed-schema-v2
fields (NULL/empty where unsupported) with non-empty `raw_extraction_json`.

- [ ] T025 [US3] Create notebook `databricks/notebooks/03_read_binary.py` reading `raw_pdfs/` via `binaryFile` (capture `path`, `content`, `length`, `modificationTime`), join back to `pdf_input_urls` on `local_pdf_path` (handle unmatched staged PDFs without crashing), validate file size > 0 and path; write `READ_BINARY` audit rows
- [ ] T026 [US3] Create notebook `databricks/notebooks/04_parse_documents.py` running `ai_parse_document(content, map('version','2.0'))` on **one** binary PDF first; store output/status in `databricks_arrow_cata.main.pdf_parsed_documents` with `parse_status` ∈ {`PARSED`,`PARSE_FAILED`,`EMPTY_DOCUMENT`} and `parse_error`; never stop the batch on a single parse failure; write `PARSE_DOCUMENT` audit rows
- [ ] T027 [US3] Create notebook `databricks/notebooks/05_extract_fixed_schema.py` running `ai_query(..., responseFormat => '{"type":"json_object"}', failOnError => false)` on **one** parsed PDF first, using the strict instructions and fixed schema v2 from [contracts/extraction_schema_v2.md](./contracts/extraction_schema_v2.md); preserve raw model output as `raw_extraction_json`, set `schema_version = 'v2'`, and record `model_endpoint`; write `AI_EXTRACT` audit rows
- [ ] T028 [US3] Create `databricks/sql/checks/07_extract_check.sql` and run via MCP: confirm ≥1 row has `parse_status = PARSED`, all fixed-schema-v2 fields present (NULL/empty allowed), `raw_extraction_json` non-empty, and no `url_part_hint` value copied into `part_number` unless content-confirmed (SC-004)

**Checkpoint**: One PDF flows binary → parsed → fixed-schema result. Stop & validate before US4.

---

## Phase 6 (US4, P2): Validate, normalize & persist results (idempotent) + full audit

**Goal**: Validate/normalize the AI output, assign `extraction_status`, and MERGE final rows into the
results table by `doc_id` with no duplicates; preserve raw JSON even on failure (PLAN.md Phases 9–10,
§14–§15; constitution Principles VII, IX, X).

**Independent test**: Each result row has a valid `extraction_status`; `VALIDATION_FAILED` rows keep
raw JSON; re-running the writer creates no duplicate `doc_id`.

- [ ] T029 [US4] Create notebook `databricks/notebooks/06_validate_normalize.py`: validate fixed root fields exist, scalars/arrays (`part_number_candidates`, `manufacturer_candidates`)/maps (5 specification maps) are well-formed, `extraction_confidence` numeric where present; convert empty strings → NULL; preserve raw JSON even if validation fails; write `VALIDATE_JSON` audit rows
- [ ] T030 [US4] In `06_validate_normalize.py`, assign `extraction_status` (`SUCCESS` / `PARTIAL_SUCCESS` / `VALIDATION_FAILED` / `FAILED`) per [contracts/extraction_schema_v2.md](./contracts/extraction_schema_v2.md) status logic (core fields = `part_number`, `manufacturer`, `description`) and capture `extraction_error`
- [ ] T031 [US4] Create notebook `databricks/notebooks/07_write_results.py` performing `MERGE INTO databricks_arrow_cata.main.datasheet_extraction_results ... ON doc_id` (idempotent; no duplicate successes), preserving all metadata carry-over columns, fixed-schema-v2 fields, `raw_extraction_json`, `schema_version`, `extraction_status`, `extraction_error`, `processed_at`; write `WRITE_RESULT` audit rows
- [ ] T032 [US4] Create `databricks/sql/checks/10_results_check.sql` and run via MCP: confirm one row per processed `doc_id` (zero duplicates after a re-run), valid `extraction_status` values, raw JSON retained on `VALIDATION_FAILED`, and an audit row exists for every executed step per `doc_id` (SC-005, SC-006, SC-009)

**Checkpoint**: Results persisted idempotently with complete audit trail. Stop & validate before US5.

---

## Phase 7 (US5, P2): Quality gate before full processing

**Goal**: After the 1-PDF and ≤5-PDF batches, compute outcome counts and core-field NULL rates and
emit a STOP/PASS signal; block the full run when most core fields are NULL (PLAN.md Phase 11, §17;
constitution Principle IV).

**Independent test**: The gate produces all required counts and a clear stop/go decision before any
full-URL processing.

- [ ] T033 [US5] Create notebook `databricks/notebooks/08_quality_gate.py` computing counts of `SUCCESS`/`PARTIAL_SUCCESS`/`FAILED`/`VALIDATION_FAILED`, and counts where `part_number`/`manufacturer`/`description`/all-three-core are NULL over the processed batch
- [ ] T034 [US5] In `08_quality_gate.py`, produce a quality summary and emit PASS/STOP: if all-core-NULL exceeds half the batch → STOP with a recommendation to improve schema/instructions before full processing; write a `QUALITY_GATE` audit row (PASS/STOP + counts summary)
- [ ] T035 [US5] Create `databricks/sql/checks/11_quality_gate_check.sql` and run via MCP: confirm the summary counts are produced and a `QUALITY_GATE` audit row exists for the batch (SC-008)

**Checkpoint**: Gate run and decision recorded; full run blocked unless PASS. Stop & validate.

---

## Phase 8 (US6, P3): Retry failed/incomplete rows without duplication

**Goal**: Reprocess only retry candidates keyed by `doc_id`, incrementing `retry_count`, keeping the
latest error, and never duplicating successful results (PLAN.md Phase 12, §18; constitution
Principle X).

**Independent test**: Only failed/incomplete rows reprocess, `retry_count` increases, and previously
successful rows are untouched and not duplicated.

- [ ] T036 [US6] Create notebook `databricks/notebooks/09_retry.py` selecting retry candidates: `input_status = DOWNLOAD_FAILED`, `parse_status = PARSE_FAILED`, or `extraction_status IN (FAILED, VALIDATION_FAILED)`, keyed by `doc_id`
- [ ] T037 [US6] In `09_retry.py`, re-invoke the appropriate phase(s) (acquire/parse/extract/validate/write) for candidates only, increment `retry_count`, keep the latest `error_message`/`extraction_error`, and rely on the `MERGE`-by-`doc_id` writer to avoid duplicate successes; append retry audit rows with incremented `retry_count`
- [ ] T038 [US6] Create `databricks/sql/checks/12_retry_check.sql` and run via MCP: confirm only candidate `doc_id`s were reprocessed, `retry_count` incremented, prior successful rows unchanged, and zero duplicate `doc_id` in results (SC-009)

**Checkpoint**: Failed rows independently retryable; successes preserved. Stop & validate.

---

## Phase 9: Polish & Cross-Cutting Concerns

**Purpose**: Final end-to-end verification and documentation; no new pipeline scope.

- [ ] T039 [P] Run the full small-batch end-to-end pass (US1→US5) on ≤5 PDFs inside Databricks per [quickstart.md](./quickstart.md) and confirm the flow `pdf_input.txt → pdf_input_urls → raw_pdfs → pdf_parsed_documents → datasheet_extraction_results → pdf_extraction_audit` (SC-010, PLAN.md §20 Definition of Done)
- [ ] T040 [P] Verify audit coverage: every executed step (`READ_INPUT_FILE`…`QUALITY_GATE`) has rows in `pdf_extraction_audit` with timing; record evidence in `databricks/sql/checks/99_audit_coverage.sql`
- [ ] T041 [P] Confirm secret safety end-to-end: `git status`/`git ls-files` show no `.databrickscfg`/tokens tracked; only `.databrickscfg.example` is committed (Principle III)
- [ ] T042 Add a short `databricks/README.md` mapping each notebook/SQL asset to its PLAN.md phase, the run order, and how to re-run/retry; reference `PLAN.md` and the constitution as governance

---

## Dependencies & Story Completion Order

```text
Setup (Phase 1: T001–T011)
        │
        ▼
Foundational (Phase 2: T012–T015)   ← BLOCKS all user stories
        │
        ▼
US1 (Phase 3: T016–T020)            ← control table populated
        │
        ▼
US2 (Phase 4: T021–T024)            ← needs doc_id rows from US1
        │
        ▼
US3 (Phase 5: T025–T028)            ← needs PDFs in raw_pdfs from US2
        │
        ▼
US4 (Phase 6: T029–T032)            ← needs AI output from US3
        │
        ▼
US5 (Phase 7: T033–T035)            ← needs results from US4 (gate before full run)
        │
        ▼
US6 (Phase 8: T036–T038)            ← retries any prior failures by doc_id
        │
        ▼
Polish (Phase 9: T039–T042)
```

The pipeline is intentionally **sequential by data dependency** (each phase consumes the prior
phase's Delta/Volume output), which is exactly what makes each phase independently runnable and
verifiable in isolation (Principle XI). US6 (retry) depends only on the existence of failed rows from
any earlier story and can be run repeatedly.

## Parallel Opportunities

Within phases, `[P]` tasks touch different files and can run together:

- **Setup**: T002, T003 (local files) in parallel; T007–T010 (independent MCP path validations) in parallel.
- **Polish**: T039, T040, T041 in parallel (independent verifications).

Across phases there is little parallelism by design — the data dependency chain is the point. Author
the notebook files (`01`–`09`) in any order locally, but **execute and validate strictly in phase
order** inside Databricks.

## Implementation Strategy

- **MVP = US1 + US2 + US3** (Phases 3–5): proves the core value — `pdf_input.txt` → control table →
  staged/downloaded PDF → parsed → fixed-schema-v2 extraction for one PDF. This satisfies the heart of
  PLAN.md's loop on a single document.
- **Increment 2 = US4 + US5** (Phases 6–7): durable idempotent results + the free-tier quality gate;
  reaches PLAN.md's Definition of Done on a small batch.
- **Increment 3 = US6** (Phase 8): operational resilience (retry) for repeated free-tier runs.
- Implement **one phase per `/speckit-implement`**, stopping at each checkpoint to run that phase's
  in-workspace validation before starting the next. Do not process the full URL list until US5 PASSes.

## Notes

- File paths `databricks/notebooks/0X_*.py` and `databricks/sql/...` mirror plan.md's Structure
  Decision; `09_retry.py` implements PLAN.md §18 retry (within scope, not new scope).
- `ai_parse_document` requires DBR 17.1+ and `ai_query` needs the workspace model endpoint — both
  confirmed in Setup (T011) before US3.
