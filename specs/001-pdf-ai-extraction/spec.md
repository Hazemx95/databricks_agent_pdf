# Feature Specification: Databricks PDF URL Looping & Fixed-Schema AI Extraction POC

**Feature Branch**: `001-pdf-ai-extraction`

**Created**: 2026-06-21

**Status**: Draft

**Input**: User description: "Build a Databricks PDF URL looping and fixed-schema AI extraction POC that reads PDF URLs from `pdf_input.txt`, supports both automatic download and manual/staged PDF modes, parses each PDF, extracts product/datasheet fields using a fixed schema, validates the output, and writes results plus a full audit trail into Unity Catalog Delta tables — running entirely inside the Databricks workspace, starting with a small batch gated by a quality check."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Load and register PDF URLs from the input file (Priority: P1)

A data engineer points the pipeline at the canonical input file
`/Volumes/databricks_arrow_cata/main/pdf_ai/input_urls/pdf_input.txt`. The pipeline reads every
line, extracts URLs from both plain text and HTML anchor tags, cleans and normalizes them,
removes exact duplicates, derives filename-based metadata, and registers one row per cleaned URL
in the control table — without relying on the old `pdf_urls.txt` path or any hardcoded local URL.

**Why this priority**: Nothing downstream can run without a populated, deduplicated control
table. This is the entry point and the single source of truth for which documents get processed.

**Independent Test**: Run only this stage against `pdf_input.txt` and confirm
`databricks_arrow_cata.main.pdf_input_urls` contains one row per unique cleaned URL, with
`source_url` preserved, duplicates marked `SKIPPED_DUPLICATE`, malformed lines marked
`INVALID_URL`, and valid rows marked `NEW`.

**Acceptance Scenarios**:

1. **Given** `pdf_input.txt` contains plain `http://` and `https://` URLs, **When** the URL load
   stage runs, **Then** each valid URL is cleaned and inserted as one `NEW` row with derived
   `source_file_name`, `supplier_folder`, and `url_part_hint`.
2. **Given** a line contains an HTML anchor tag such as `<a href="http://.../skupage.x.pdf">...</a>`,
   **When** the stage runs, **Then** the `href` value is extracted, cleaned, and registered.
3. **Given** the same cleaned URL appears more than once, **When** the stage runs, **Then** only
   the first occurrence is `NEW` and later exact duplicates are recorded as `SKIPPED_DUPLICATE`.
4. **Given** a URL has trailing punctuation (comma, semicolon, parenthesis, whitespace), **When**
   cleaning runs, **Then** the trailing characters are removed before deduplication.
5. **Given** a line cannot be parsed into a valid URL, **When** the stage runs, **Then** the row
   is recorded as `INVALID_URL` with the original `source_url` preserved and the batch continues.

---

### User Story 2 - Obtain each PDF via download or manual staging (Priority: P1)

For every registered URL the pipeline makes the PDF available in
`/Volumes/databricks_arrow_cata/main/pdf_ai/raw_pdfs/`. In automatic mode it downloads the PDF
using a deterministic filename. In manual/staged mode — used because free Databricks may block
outbound access, and the user has already uploaded PDFs into `raw_pdfs/` — it detects existing
files and matches them back to control rows. A staged file that cannot be confidently matched is
preserved as a staged record and still processed with available metadata. A single failed URL or
file never stops the batch.

**Why this priority**: A PDF must physically exist in the Volume before any parsing or extraction
can occur, and the manual/staged path is a first-class supported mode, not an error case.

**Independent Test**: With outbound download disabled and PDFs pre-uploaded to `raw_pdfs/`, run
this stage and confirm control rows reach `MANUALLY_STAGED` (or `DOWNLOADED` when download works),
`local_pdf_path` is populated, pre-existing files are never deleted, and download failures are
recorded as `DOWNLOAD_FAILED` while remaining URLs continue.

**Acceptance Scenarios**:

1. **Given** outbound access works and a row is `NEW`, **When** download runs, **Then** the PDF is
   saved to `raw_pdfs/` with a deterministic name, `local_pdf_path` is set, and status becomes
   `DOWNLOADED` (transitioning through `DOWNLOADING`).
2. **Given** outbound access is blocked, **When** download is attempted, **Then** the row is marked
   `DOWNLOAD_FAILED` with an error message and the batch continues to the next URL.
3. **Given** PDFs were manually uploaded into `raw_pdfs/`, **When** the staging detection runs,
   **Then** matching files are linked to their control rows and marked `MANUALLY_STAGED` using
   deterministic name, `source_file_name`, `url_part_hint`, or available filename metadata.
4. **Given** a staged PDF cannot be confidently matched to any URL, **When** staging runs, **Then**
   it is preserved as a staged file record and processed with available metadata rather than
   discarded.
5. **Given** any existing file in `raw_pdfs/`, **When** the staging stage runs, **Then** no
   existing PDF is deleted.

---

### User Story 3 - Read, parse, and AI-extract each PDF into the fixed schema (Priority: P1)

For each available PDF the pipeline reads the file as binary, parses its content using
Databricks-native document parsing, and extracts product/datasheet information into fixed schema
v2. Every PDF yields the same schema regardless of document type (datasheet, spec sheet, manual,
product page, connector spec, SKU page). The AI must not invent values, must treat URL/filename
values as hints only, and must preserve its raw output.

**Why this priority**: This is the core value of the POC — turning heterogeneous PDFs into a
single, predictable structured shape.

**Independent Test**: Given at least one staged PDF, run binary read → parse → extract and confirm
`pdf_parsed_documents` holds a parse status and `datasheet_extraction_results` holds a row with
every fixed-schema-v2 field present (NULL/empty where unsupported) plus `raw_extraction_json`.

**Acceptance Scenarios**:

1. **Given** a PDF exists in `raw_pdfs/`, **When** the binary read runs, **Then** the file's path,
   content, length, and modification time are obtained and joined back to the control row.
2. **Given** binary content is available, **When** parsing runs, **Then** parsed output or a parse
   status (`PARSED`, `PARSE_FAILED`, or `EMPTY_DOCUMENT`) is written to `pdf_parsed_documents`, and
   a parse error never stops the batch.
3. **Given** parsed content, **When** AI extraction runs, **Then** the result conforms to fixed
   schema v2 with the same fields for every PDF, missing scalars as NULL, and missing arrays/maps
   as NULL or empty.
4. **Given** the document contains multiple candidate part numbers, **When** extraction runs,
   **Then** they populate `part_number_candidates`, and `part_number` is filled only when one is
   clearly the main part number.
5. **Given** a value appears only in the URL or filename, **When** extraction runs, **Then** it is
   not promoted into an extracted field unless the PDF content confirms it; URL/filename values
   remain metadata hints.
6. **Given** any extraction, **When** it completes, **Then** the raw AI output is preserved in
   `raw_extraction_json`.

---

### User Story 4 - Validate, normalize, and persist results with full audit (Priority: P2)

After extraction the pipeline validates the AI output, normalizes it (empty strings → NULL,
arrays/maps confirmed well-formed, confidence numeric when present), assigns an extraction status,
and writes the final row to the results table. Every major step writes an audit record capturing
status, error, and timing — even when the raw output failed validation, which is still preserved.

**Why this priority**: Durable, validated results and a complete audit trail are required for the
POC to be trustworthy and diagnosable, but they depend on extraction (P1) existing first.

**Independent Test**: Feed known-good and known-bad AI outputs through validation and confirm rows
land in `datasheet_extraction_results` with the correct `extraction_status`, raw JSON preserved on
failure, and an audit row in `pdf_extraction_audit` for each executed step.

**Acceptance Scenarios**:

1. **Given** valid AI output with core fields present, **When** validation runs, **Then** the row
   is written with `extraction_status = SUCCESS`.
2. **Given** valid structure but one or more core business fields are NULL, **When** validation
   runs, **Then** status is `PARTIAL_SUCCESS`.
3. **Given** AI output with invalid structure or incompatible types, **When** validation runs,
   **Then** status is `VALIDATION_FAILED` and `raw_extraction_json` is still preserved.
4. **Given** extraction or parsing failed entirely, **When** the result is written, **Then** status
   is `FAILED` with the error captured.
5. **Given** any executed step (`READ_INPUT_FILE`, `CLEAN_URLS`, `INSERT_CONTROL_TABLE`,
   `DOWNLOAD_PDF`, `STAGE_PDF`, `READ_BINARY`, `PARSE_DOCUMENT`, `AI_EXTRACT`, `VALIDATE_JSON`,
   `WRITE_RESULT`, `QUALITY_GATE`), **When** it runs, **Then** an audit row records step name,
   status, error message (if any), and timing.

---

### User Story 5 - Quality gate before full processing (Priority: P2)

Because the POC runs on free Databricks, the operator processes 1 PDF first, then up to 5, and runs
a quality gate before processing all URLs. The gate reports outcome counts and core-field NULL
rates. If most fields are NULL, full processing stops until the schema or extraction instructions
improve.

**Why this priority**: It protects limited free-tier quota and prevents a low-quality full run, but
only makes sense once the end-to-end path (P1) produces results.

**Independent Test**: After a 1–5 PDF batch, run the quality gate and confirm it produces the
required counts and a clear stop/go signal that blocks full processing when most fields are NULL.

**Acceptance Scenarios**:

1. **Given** a first batch of 1 PDF has completed, **When** the quality gate runs, **Then** it
   reports counts of `SUCCESS`, `PARTIAL_SUCCESS`, and `FAILED`, plus counts where `part_number`,
   `manufacturer`, `description`, and all core extracted fields are NULL.
2. **Given** most core fields are NULL across the first batch, **When** the gate evaluates results,
   **Then** it signals to stop full processing and to improve the schema/instructions first.
3. **Given** first-batch quality is acceptable, **When** the gate passes, **Then** processing may
   expand to up to 5 PDFs and then to full processing.
4. **Given** the operator has not run the gate, **When** full processing is requested, **Then** the
   workflow expects the gate to run before all URLs are processed.

---

### User Story 6 - Retry failed or incomplete rows without duplication (Priority: P3)

The operator re-runs the pipeline to recover failed or incomplete documents. Only retry candidates
are reprocessed, keyed by `doc_id`; `retry_count` increments, the latest error is kept, and
successful results are never duplicated.

**Why this priority**: Retry makes the POC operationally resilient on a flaky free tier, but it is
an enhancement over a working forward path.

**Independent Test**: Mark some rows as failed (`DOWNLOAD_FAILED`, `PARSE_FAILED`, or
`extraction_status IN (FAILED, VALIDATION_FAILED)`), re-run, and confirm only those rows are
reprocessed, `retry_count` increases, and previously successful rows are untouched and not
duplicated.

**Acceptance Scenarios**:

1. **Given** rows with `input_status = DOWNLOAD_FAILED`, `parse_status = PARSE_FAILED`, or
   `extraction_status IN (FAILED, VALIDATION_FAILED)`, **When** retry runs, **Then** only those
   rows are reprocessed.
2. **Given** a row is retried, **When** it reprocesses, **Then** `retry_count` increments and the
   latest error message is kept.
3. **Given** a row already succeeded, **When** retry runs, **Then** its result is not duplicated and
   not overwritten, using `doc_id` as the processing key.

---

### Edge Cases

- A line in `pdf_input.txt` is blank, whitespace-only, or contains text but no extractable URL →
  recorded as `INVALID_URL`; batch continues.
- An HTML anchor tag is malformed or contains multiple `href` values → the URL extraction handles
  it deterministically and, if no valid URL results, marks `INVALID_URL`.
- A PDF downloads but is zero-length or not a real PDF → parsing records `EMPTY_DOCUMENT` or
  `PARSE_FAILED` without stopping the batch.
- Two different URLs resolve to the same deterministic filename → deterministic naming includes a
  uniqueness component (e.g., `doc_id` and a URL hash) so files do not collide or overwrite.
- A staged PDF in `raw_pdfs/` matches no URL and has no useful filename metadata → preserved as a
  staged record and processed with whatever metadata is available.
- AI extraction returns text that is not valid JSON → `VALIDATION_FAILED`, raw output preserved.
- The same batch is run twice → already-successful `doc_id` rows are not duplicated.
- Outbound access is available for some URLs and blocked for others within one run → each row is
  resolved independently (`DOWNLOADED` vs `DOWNLOAD_FAILED`/`MANUALLY_STAGED`).

## Requirements *(mandatory)*

### Functional Requirements

**Input ingestion & URL handling**

- **FR-001**: The pipeline MUST read the full contents of
  `/Volumes/databricks_arrow_cata/main/pdf_ai/input_urls/pdf_input.txt` as the single source of
  truth and MUST NOT use the old `pdf_urls.txt` path or hardcoded local URLs.
- **FR-002**: The pipeline MUST extract URLs from both plain `http://`/`https://` text and HTML
  anchor tags (by extracting the `href` value).
- **FR-003**: The pipeline MUST clean and normalize each URL by removing trailing punctuation
  (comma, semicolon, parenthesis) and surrounding whitespace.
- **FR-004**: The pipeline MUST deduplicate exact cleaned URLs, registering the first occurrence as
  `NEW` and later exact duplicates as `SKIPPED_DUPLICATE`.
- **FR-005**: The pipeline MUST preserve the original `source_url` for traceability on every row.
- **FR-006**: The pipeline MUST derive `source_file_name` (filename from the URL path),
  `supplier_folder` (folder segment ending in an underscore, e.g. `ech_`, `ittca_`, `wdm_`,
  `fackg_`), and `url_part_hint` (a weak candidate derived from the filename only).
- **FR-007**: The pipeline MUST mark unparseable rows as `INVALID_URL` and continue processing the
  remaining lines.
- **FR-008**: The pipeline MUST treat `url_part_hint` as a metadata hint only and MUST NOT treat it
  as the confirmed `part_number` unless PDF content supports it.

**Control table**

- **FR-009**: The pipeline MUST insert one row per cleaned URL into
  `databricks_arrow_cata.main.pdf_input_urls` with the columns and status values defined in Key
  Entities.

**PDF acquisition (two modes)**

- **FR-010**: In automatic download mode, the pipeline MUST download each `NEW` URL's PDF into
  `/Volumes/databricks_arrow_cata/main/pdf_ai/raw_pdfs/` using a deterministic filename, update
  `local_pdf_path`, and transition status `NEW → DOWNLOADING → DOWNLOADED`.
- **FR-011**: In manual/staged mode, the pipeline MUST detect PDFs already present in `raw_pdfs/`
  and match them to control rows using deterministic name, `source_file_name`, `url_part_hint`, or
  available filename metadata, marking matched rows `MANUALLY_STAGED`.
- **FR-012**: The pipeline MUST preserve, as a staged file record processed with available
  metadata, any staged PDF that cannot be confidently matched to a URL.
- **FR-013**: The pipeline MUST NOT delete existing PDFs in `raw_pdfs/`.
- **FR-014**: On download failure, the pipeline MUST mark the row `DOWNLOAD_FAILED` with an error
  message and MUST continue the batch; it MUST NOT stop the full batch because one URL or one PDF
  fails.
- **FR-015**: When outbound access is blocked, the pipeline MUST keep failed rows retryable and
  continue the rest of the pipeline from staged PDFs.

**Binary read & parsing**

- **FR-016**: The pipeline MUST read PDFs from `raw_pdfs/` as binary files (obtaining path,
  content, length, modification time) and join them back to the control table.
- **FR-017**: The pipeline MUST parse PDF content using Databricks-native document parsing
  (`ai_parse_document(content, map('version', '2.0'))`) when available, storing parsed output or a
  parse status in `databricks_arrow_cata.main.pdf_parsed_documents` with status values `PARSED`,
  `PARSE_FAILED`, or `EMPTY_DOCUMENT`.
- **FR-018**: The pipeline MUST capture parse errors without stopping the batch.

**AI extraction (fixed schema v2)**

- **FR-019**: The pipeline MUST extract product/datasheet information into fixed schema v2 (see Key
  Entities) and MUST return the same schema for every PDF regardless of document type.
- **FR-020**: The pipeline MUST set missing scalar fields to NULL and missing arrays/maps to NULL
  or empty structures.
- **FR-021**: The pipeline MUST NOT invent values; it MUST use only values supported by the PDF
  content, and treat URL/filename values as hints only.
- **FR-022**: The pipeline MUST populate `part_number_candidates` when multiple candidate part
  numbers exist, and fill `part_number` only when one is clearly the main one.
- **FR-023**: The pipeline MUST group specifications into `electrical`, `mechanical`,
  `environmental`, `compliance`, and `general` specification maps, keeping units inside values.
- **FR-024**: The pipeline MUST preserve the raw AI output in `raw_extraction_json`.

**Validation & results**

- **FR-025**: The pipeline MUST validate that AI output is valid structured JSON, that all fixed
  root fields exist, that arrays are arrays and maps are valid key-value structures, and that
  confidence is numeric when present.
- **FR-026**: The pipeline MUST normalize output by converting empty strings to NULL.
- **FR-027**: The pipeline MUST preserve `raw_extraction_json` even when validation fails.
- **FR-028**: The pipeline MUST assign `extraction_status` as `SUCCESS` (core fields extracted,
  schema valid), `PARTIAL_SUCCESS` (schema valid but one or more core business fields NULL),
  `VALIDATION_FAILED` (invalid structure/types), or `FAILED` (extraction or parse failed).
- **FR-029**: The pipeline MUST write final structured rows into
  `databricks_arrow_cata.main.datasheet_extraction_results` with the columns defined in Key
  Entities, including `schema_version`.

**Audit**

- **FR-030**: The pipeline MUST write an audit record to
  `databricks_arrow_cata.main.pdf_extraction_audit` for every executed major step:
  `READ_INPUT_FILE`, `CLEAN_URLS`, `INSERT_CONTROL_TABLE`, `DOWNLOAD_PDF`, `STAGE_PDF`,
  `READ_BINARY`, `PARSE_DOCUMENT`, `AI_EXTRACT`, `VALIDATE_JSON`, `WRITE_RESULT`, `QUALITY_GATE`.
- **FR-031**: Each audit record MUST capture step name, status, error message (when applicable),
  retry count, and timing.

**Quality gate**

- **FR-032**: The pipeline MUST support staged processing: 1 PDF first, then up to 5 PDFs, then
  full processing only after the quality gate passes.
- **FR-033**: The quality gate MUST report counts of `SUCCESS`, `PARTIAL_SUCCESS`, `FAILED`, and
  counts where `part_number`, `manufacturer`, `description`, and all core extracted fields are NULL.
- **FR-034**: When most fields are NULL, the quality gate MUST signal to stop full processing until
  the schema or extraction instructions are improved.

**Retry**

- **FR-035**: The pipeline MUST retry only failed/incomplete rows — `input_status =
  DOWNLOAD_FAILED`, `parse_status = PARSE_FAILED`, or `extraction_status IN (FAILED,
  VALIDATION_FAILED)` — keyed by `doc_id`.
- **FR-036**: On retry, the pipeline MUST increment `retry_count`, keep the latest error message,
  and avoid duplicating successful results.

**Execution environment**

- **FR-037**: All real pipeline logic MUST execute inside the Databricks workspace; the local
  machine is used only for editing, Git, Spec Kit, MCP connection, and orchestration.
- **FR-038**: All durable inputs and outputs MUST use Unity Catalog objects under
  `databricks_arrow_cata.main` and the `pdf_ai` Volume.
- **FR-039**: Each phase MUST be independently implementable and testable inside Databricks before
  the next phase begins.

### Key Entities *(include if feature involves data)*

- **PDF Input URL (control row)** — `databricks_arrow_cata.main.pdf_input_urls`: one row per
  cleaned URL. Attributes: `doc_id` (BIGINT, generated identity, processing key), `source_url`,
  `cleaned_url`, `source_file_name`, `supplier_folder`, `url_part_hint`, `input_status`,
  `local_pdf_path`, `error_message`, `created_at`, `updated_at`. `input_status` ∈ {`NEW`,
  `SKIPPED_DUPLICATE`, `INVALID_URL`, `DOWNLOADING`, `DOWNLOADED`, `DOWNLOAD_FAILED`,
  `MANUALLY_STAGED`}.

- **Parsed Document** — `databricks_arrow_cata.main.pdf_parsed_documents`: parse outcome per
  document. Attributes: `doc_id`, `cleaned_url`, `local_pdf_path`, `parsed_content` (string or
  variant), `parse_status`, `parse_error`, `parsed_at`. `parse_status` ∈ {`PARSED`, `PARSE_FAILED`,
  `EMPTY_DOCUMENT`}.

- **Datasheet Extraction Result** — `databricks_arrow_cata.main.datasheet_extraction_results`:
  final fixed-schema-v2 output per document. Metadata attributes: `doc_id`, `source_url`,
  `cleaned_url`, `source_file_name`, `supplier_folder`, `url_part_hint`, `local_pdf_path`.
  Fixed schema v2 attributes: `document_title`, `document_type`, `part_number`, `manufacturer`,
  `description`, `product_family`, `product_category`, `part_number_candidates` (array),
  `manufacturer_candidates` (array), `electrical_specifications`, `mechanical_specifications`,
  `environmental_specifications`, `compliance_specifications`, `general_specifications` (each a
  map<string,string>), `extraction_confidence` (double), `extraction_notes`. Provenance/control
  attributes: `raw_extraction_json`, `schema_version`, `extraction_status`, `extraction_error`,
  `processed_at`. `extraction_status` ∈ {`SUCCESS`, `PARTIAL_SUCCESS`, `VALIDATION_FAILED`,
  `FAILED`}.

- **Audit Record** — `databricks_arrow_cata.main.pdf_extraction_audit`: one row per executed step
  per document. Attributes: `audit_id` (BIGINT, generated identity), `doc_id`, `cleaned_url`,
  `step_name`, `status`, `error_message`, `retry_count`, `started_at`, `finished_at`,
  `duration_seconds`, `model_endpoint`, `created_at`. `step_name` ∈ {`READ_INPUT_FILE`,
  `CLEAN_URLS`, `INSERT_CONTROL_TABLE`, `DOWNLOAD_PDF`, `STAGE_PDF`, `READ_BINARY`,
  `PARSE_DOCUMENT`, `AI_EXTRACT`, `VALIDATE_JSON`, `WRITE_RESULT`, `QUALITY_GATE`}.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Running the pipeline against `pdf_input.txt` produces exactly one control-table row
  per unique cleaned URL, with zero exact-duplicate `NEW` rows and every original `source_url`
  preserved.
- **SC-002**: Both plain URLs and HTML anchor-tag URLs present in the input file are successfully
  extracted and registered (100% of well-formed entries of each type).
- **SC-003**: With outbound download blocked and PDFs pre-staged in `raw_pdfs/`, the pipeline still
  completes end-to-end for the staged PDFs and never deletes an existing staged file.
- **SC-004**: At least one PDF is read as binary, parsed, and AI-extracted into fixed schema v2,
  with every fixed-schema-v2 field present (NULL/empty where unsupported) and `raw_extraction_json`
  retained.
- **SC-005**: Every extraction result row carries a valid `extraction_status` from the defined set,
  and every row that failed validation still retains its raw AI output.
- **SC-006**: For every executed step across the run, a corresponding audit row exists in
  `pdf_extraction_audit` — no major step runs without an audit record.
- **SC-007**: A single failing URL or PDF never aborts the batch; remaining items continue to be
  processed.
- **SC-008**: The quality gate runs on the first batch (1, then up to 5 PDFs) and emits the
  required counts before any full-URL processing begins.
- **SC-009**: Re-running the pipeline reprocesses only retry-candidate rows, increments their
  `retry_count`, and produces no duplicate successful result rows (verified by unique `doc_id`).
- **SC-010**: The complete flow — `pdf_input.txt → control table → PDFs in raw_pdfs → parsed
  documents → fixed-schema results → audit table` — is demonstrated running inside the Databricks
  workspace on a small batch.

## Assumptions

- The Unity Catalog objects exist or will be created as named in PLAN.md: catalog
  `databricks_arrow_cata`, schema `main`, Volume `pdf_ai`, with `input_urls/pdf_input.txt` and the
  `raw_pdfs/` folder present.
- The user has already manually uploaded one or more PDFs into `raw_pdfs/`; manual/staged mode is a
  fully supported path, not merely an error fallback.
- Free Databricks may block outbound access to external hosts (e.g.,
  `download.siliconexpert.com`); the pipeline must degrade to staged mode rather than fail.
- Databricks-native `ai_parse_document` and AI extraction (`ai_extract`/equivalent) functions are
  available in the workspace; if a native parser is unavailable, the parse status path still
  records the outcome without breaking the batch.
- "Core extracted fields" for quality-gate and status purposes are `part_number`, `manufacturer`,
  and `description`, consistent with PLAN.md's quality-gate checks.
- `schema_version` is recorded on every result row to identify which fixed-schema version produced
  it (PLAN.md references fixed schema v2).
- Deterministic download filenames include a uniqueness component (e.g.,
  `{doc_id}_{url_hash}_{source_file_name}`) to prevent collisions, per PLAN.md's suggested format.
- An optional `debug/` folder under the Volume may hold debug or failed-extraction samples; it is
  not required for acceptance.
- The initial small batch size is 1 PDF, then up to 5 PDFs, before full processing, per the
  free-tier quality-gate rule.
