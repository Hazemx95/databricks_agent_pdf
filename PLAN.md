# PLAN.md — Databricks PDF URL Looping and Fixed-Schema AI Extraction POC

## 1. Current confirmed setup

This POC is running in Databricks using Unity Catalog objects under:

```text
Catalog: databricks_arrow_cata
Schema: main
Volume: pdf_ai
```

The main Volume path is:

```text
/Volumes/databricks_arrow_cata/main/pdf_ai/
```

The input URL file already exists at:

```text
/Volumes/databricks_arrow_cata/main/pdf_ai/input_urls/pdf_input.txt
```

This file contains all PDF URLs that must be processed. It is the source of truth for this POC.

The raw PDF staging folder already exists at:

```text
/Volumes/databricks_arrow_cata/main/pdf_ai/raw_pdfs/
```

The pipeline must use `pdf_input.txt`, not the old `pdf_urls.txt` name.

---

## 2. Objective

Build a Databricks pipeline that loops through all PDF URLs stored in:

```text
/Volumes/databricks_arrow_cata/main/pdf_ai/input_urls/pdf_input.txt
```

For each URL, the pipeline must:

1. Read the URL from the input text file.
2. Clean and normalize the URL.
3. Support plain URLs and HTML anchor-tag URLs.
4. Deduplicate URLs.
5. Register each URL in a Delta control table.
6. Download or stage each PDF into the raw PDF Volume folder.
7. Read the downloaded PDFs as binary files.
8. Parse PDF content.
9. Extract product/datasheet fields using AI with a fixed schema.
10. Validate and normalize the AI JSON output.
11. Write the final structured result into a Delta table.
12. Write audit/error information for every step.

The POC must prove this full flow:

```text
pdf_input.txt
    ↓
loop URLs
    ↓
clean + deduplicate
    ↓
control Delta table
    ↓
download/stage PDFs into raw_pdfs Volume
    ↓
read_files(binaryFile)
    ↓
ai_parse_document
    ↓
ai_extract fixed schema
    ↓
validate JSON
    ↓
Delta result table
    ↓
audit table
```

---

## 3. Important note for Claude / Speckit

Do not assume PDFs are already manually uploaded.

The first implementation must read URLs from:

```text
/Volumes/databricks_arrow_cata/main/pdf_ai/input_urls/pdf_input.txt
```

Then it must loop through the URL list and create the processing logic.

The plan must support two modes:

### Mode 1 — Automatic URL download mode

Databricks reads URLs from `pdf_input.txt`, downloads PDFs, and saves them into:

```text
/Volumes/databricks_arrow_cata/main/pdf_ai/raw_pdfs/
```

### Mode 2 — Manual/staged PDF mode

If free Databricks blocks outbound access to `download.siliconexpert.com`, the user can manually upload PDFs into:

```text
/Volumes/databricks_arrow_cata/main/pdf_ai/raw_pdfs/
```

The rest of the pipeline must continue from the raw PDF Volume folder.

---

## 4. Environment and access

The POC uses a free Databricks workspace.

The coding agent should work through MCP server access to the Databricks workspace.

Workspace profile configuration is stored locally in:

```text
.databrickscfg
```

Rules:

```text
- Use .databrickscfg for workspace profile access.
- Never commit .databrickscfg.
- Never commit Databricks tokens or secrets.
- Commit only .databrickscfg.example if needed, with placeholder values only.
```

Because this is free Databricks, implementation must start with a small batch first, for example 1 to 5 PDFs, then expand after validation.

---

## 5. Unity Catalog paths

### Input URLs

```text
/Volumes/databricks_arrow_cata/main/pdf_ai/input_urls/pdf_input.txt
```

Purpose:

```text
Contains all PDF URLs to process.
```

### Raw PDFs

```text
/Volumes/databricks_arrow_cata/main/pdf_ai/raw_pdfs/
```

Purpose:

```text
Stores downloaded or manually staged PDF files.
```

### Optional parsed/debug output

```text
/Volumes/databricks_arrow_cata/main/pdf_ai/debug/
```

Purpose:

```text
Optional location for debug files, sample parsed text, or failed extraction samples.
```

---

## 6. Delta tables

Create all tables under:

```text
databricks_arrow_cata.main
```

Required tables:

```text
databricks_arrow_cata.main.pdf_input_urls
databricks_arrow_cata.main.pdf_parsed_documents
databricks_arrow_cata.main.datasheet_extraction_results
databricks_arrow_cata.main.pdf_extraction_audit
```

---

## 7. Input control table

Table name:

```text
databricks_arrow_cata.main.pdf_input_urls
```

Purpose:

```text
One row per cleaned PDF URL from pdf_input.txt.
```

Columns:

```text
doc_id BIGINT GENERATED ALWAYS AS IDENTITY
source_url STRING
cleaned_url STRING
source_file_name STRING
supplier_folder STRING
url_part_hint STRING
input_status STRING
local_pdf_path STRING
error_message STRING
created_at TIMESTAMP
updated_at TIMESTAMP
```

Status values:

```text
NEW
SKIPPED_DUPLICATE
INVALID_URL
DOWNLOADING
DOWNLOADED
DOWNLOAD_FAILED
MANUALLY_STAGED
```

Important:

```text
url_part_hint is only metadata from the URL/file name.
It must not be treated as the final extracted part_number unless PDF content supports it.
```

---

## 8. URL reading and cleaning rules

The pipeline must read the whole content of:

```text
/Volumes/databricks_arrow_cata/main/pdf_ai/input_urls/pdf_input.txt
```

It must extract URLs even if a line contains an HTML anchor tag, for example:

```html
<a href="http://download.siliconexpert.com/pdfs2/.../skupage.m22pvt45pgb99.pdf" target="_blank">...</a>
```

Rules:

```text
- Extract href value if the row is HTML.
- Support plain http:// and https:// URLs.
- Remove trailing characters such as comma, semicolon, parenthesis, or whitespace.
- Deduplicate exact cleaned URLs.
- Mark invalid rows as INVALID_URL.
- Preserve original source_url for traceability.
```

URL metadata to derive:

```text
source_file_name:
  The filename from the URL path.

supplier_folder:
  Folder ending with underscore, such as ech_, ittca_, wdm_, fackg_.

url_part_hint:
  A weak candidate derived from the filename only.
  Example: skupage.242307.pdf → 242307
  Example: ca3102e183pf80spec.pdf → ca3102e183pf80
```

---

## 9. PDF download/staging logic

The pipeline must loop through rows in `pdf_input_urls` where:

```text
input_status = NEW
```

For each URL:

```text
1. Attempt to download the PDF.
2. Save it into raw_pdfs Volume.
3. Use deterministic file naming to avoid collisions.
4. Update local_pdf_path.
5. Update input_status.
6. Write audit record.
```

Suggested saved file format:

```text
/Volumes/databricks_arrow_cata/main/pdf_ai/raw_pdfs/{doc_id}_{url_hash}_{source_file_name}
```

If automatic download fails:

```text
- Mark row as DOWNLOAD_FAILED.
- Store the error message.
- Do not stop the whole batch.
- Continue processing remaining URLs.
```

If outbound internet is blocked in free Databricks:

```text
- Keep the failed rows for retry.
- Allow user to manually upload PDFs into raw_pdfs.
- Continue the pipeline from read_files(binaryFile).
```

---

## 10. Read PDFs as binary

Once PDFs exist in:

```text
/Volumes/databricks_arrow_cata/main/pdf_ai/raw_pdfs/
```

The pipeline reads them using:

```text
read_files with format = binaryFile
```

Expected binary file columns:

```text
path
content
length
modificationTime
```

This step confirms that Databricks can see the PDF files and read them as bytes.

---

## 11. PDF parsing

The preferred Databricks-native parser is:

```text
ai_parse_document(content, map('version', '2.0'))
```

Input:

```text
PDF binary content
```

Output:

```text
Parsed document object containing text, pages, tables, and layout information where supported.
```

The parsed result should be stored or tracked in:

```text
databricks_arrow_cata.main.pdf_parsed_documents
```

Columns:

```text
doc_id BIGINT
cleaned_url STRING
local_pdf_path STRING
parsed_content STRING or VARIANT
parse_status STRING
parse_error STRING
parsed_at TIMESTAMP
```

Status values:

```text
PARSED
PARSE_FAILED
EMPTY_DOCUMENT
```

---

## 12. Fixed extraction schema v2

The extraction schema must be stronger than the simple example schema.

The final output must not depend only on these three fields:

```text
part_number
manufacturer
description
```

Because different PDFs may be datasheets, specification sheets, manuals, product pages, connector specs, or SKU pages.

Use fixed schema v2:

```text
document_title STRING
document_type STRING
part_number STRING
manufacturer STRING
description STRING
product_family STRING
product_category STRING
part_number_candidates ARRAY<STRING>
manufacturer_candidates ARRAY<STRING>
electrical_specifications MAP<STRING, STRING>
mechanical_specifications MAP<STRING, STRING>
environmental_specifications MAP<STRING, STRING>
compliance_specifications MAP<STRING, STRING>
general_specifications MAP<STRING, STRING>
extraction_confidence DOUBLE
extraction_notes STRING
```

Important rules:

```text
- Return the same schema for every PDF.
- Missing scalar fields must be NULL.
- Missing arrays may be NULL or empty array.
- Missing maps may be NULL or empty map.
- Do not invent values.
- Use candidates arrays when the PDF contains multiple possible part numbers/manufacturers.
- Keep URL-derived values separate from AI-confirmed extracted fields.
```

---

## 13. AI extraction behavior

The AI extraction step receives:

```text
parsed document content
fixed schema v2
strict extraction instructions
```

Extraction instruction:

```text
Extract product/datasheet information from this PDF.
Use only values supported by the document content.
Do not invent values.
If a field is missing, return null.
If multiple possible part numbers exist, fill part_number_candidates.
If one part number is clearly the main one, fill part_number.
Group specifications into electrical, mechanical, environmental, compliance, and general maps.
Keep units inside values.
Return the same schema every time.
```

The raw AI output must be preserved in:

```text
raw_extraction_json
```

---

## 14. Final result table

Table name:

```text
databricks_arrow_cata.main.datasheet_extraction_results
```

Columns:

```text
doc_id BIGINT
source_url STRING
cleaned_url STRING
source_file_name STRING
supplier_folder STRING
url_part_hint STRING
local_pdf_path STRING

document_title STRING
document_type STRING
part_number STRING
manufacturer STRING
description STRING
product_family STRING
product_category STRING

part_number_candidates ARRAY<STRING>
manufacturer_candidates ARRAY<STRING>

electrical_specifications MAP<STRING, STRING>
mechanical_specifications MAP<STRING, STRING>
environmental_specifications MAP<STRING, STRING>
compliance_specifications MAP<STRING, STRING>
general_specifications MAP<STRING, STRING>

extraction_confidence DOUBLE
extraction_notes STRING
raw_extraction_json STRING
schema_version STRING
extraction_status STRING
extraction_error STRING
processed_at TIMESTAMP
```

Status values:

```text
SUCCESS
PARTIAL_SUCCESS
VALIDATION_FAILED
FAILED
```

---

## 15. Validation rules

After AI extraction:

```text
1. Validate that output is valid JSON or valid structured output.
2. Validate that all fixed root fields exist.
3. Convert empty strings to NULL.
4. Validate arrays are arrays.
5. Validate maps/specifications are valid key-value structures.
6. Validate confidence is numeric if present.
7. Preserve raw JSON even if validation fails.
```

Status logic:

```text
SUCCESS:
  Important fields are extracted and schema is valid.

PARTIAL_SUCCESS:
  Schema is valid, but one or more important business fields are null.

VALIDATION_FAILED:
  AI returned invalid structure or incompatible types.

FAILED:
  AI extraction failed or parse failed.
```

---

## 16. Audit table

Table name:

```text
databricks_arrow_cata.main.pdf_extraction_audit
```

Columns:

```text
audit_id BIGINT GENERATED ALWAYS AS IDENTITY
doc_id BIGINT
cleaned_url STRING
step_name STRING
status STRING
error_message STRING
retry_count INT
started_at TIMESTAMP
finished_at TIMESTAMP
duration_seconds DOUBLE
model_endpoint STRING
created_at TIMESTAMP
```

Required audited steps:

```text
READ_INPUT_FILE
CLEAN_URLS
INSERT_CONTROL_TABLE
DOWNLOAD_PDF
READ_BINARY
PARSE_DOCUMENT
AI_EXTRACT
VALIDATE_JSON
WRITE_RESULT
QUALITY_GATE
```

---

## 17. Quality gate before full run

Because this POC uses free Databricks, do not process all URLs first.

Start with:

```text
1 to 5 PDFs
```

Then inspect:

```text
- Count of SUCCESS
- Count of PARTIAL_SUCCESS
- Count of FAILED
- Count where part_number is NULL
- Count where manufacturer is NULL
- Count where description is NULL
- Count where all core extracted fields are NULL
```

If most fields are NULL:

```text
Stop.
Improve the fixed schema and extraction instructions.
Do not process all PDFs until first-batch quality is acceptable.
```

---

## 18. Retry design

Retry only failed or incomplete rows.

Retry candidates:

```text
input_status = DOWNLOAD_FAILED
parse_status = PARSE_FAILED
extraction_status IN (FAILED, VALIDATION_FAILED)
```

Rules:

```text
- Increment retry_count.
- Keep latest error message.
- Avoid duplicating successful results.
- Use doc_id as the processing key.
```

---

## 19. Implementation phases for Speckit

### Phase 1 — Confirm workspace and MCP setup

```text
- Verify MCP server can connect to Databricks.
- Verify .databrickscfg profile exists locally.
- Verify .databrickscfg is ignored by Git.
```

### Phase 2 — Confirm Unity Catalog paths

```text
- Confirm /Volumes/databricks_arrow_cata/main/pdf_ai/input_urls/pdf_input.txt exists.
- Confirm /Volumes/databricks_arrow_cata/main/pdf_ai/raw_pdfs/ exists.
- Confirm schema databricks_arrow_cata.main exists.
```

### Phase 3 — Load URLs from pdf_input.txt

```text
- Read pdf_input.txt from the Volume.
- Extract URLs from plain text and HTML anchor tags.
- Clean and deduplicate URLs.
- Derive source_file_name, supplier_folder, and url_part_hint.
- Insert rows into pdf_input_urls control table.
```

### Phase 4 — Download/stage PDFs

```text
- Loop through NEW URLs.
- Start with first 1 to 5 URLs for POC.
- Download PDFs into raw_pdfs Volume.
- Update status and local_pdf_path.
- Log failures without stopping the batch.
```

### Phase 5 — Read PDFs as binary

```text
- Read raw_pdfs folder using binaryFile.
- Join files back to control table by local_pdf_path.
- Validate file size and path.
```

### Phase 6 — Parse documents

```text
- Run ai_parse_document on PDF binary content.
- Store parsed output or parse status.
- Mark failed PDFs as PARSE_FAILED.
```

### Phase 7 — AI extraction with fixed schema v2

```text
- Run ai_extract on parsed PDF content.
- Use fixed schema v2.
- Preserve raw extraction JSON.
```

### Phase 8 — Validate and normalize

```text
- Flatten AI output.
- Convert empty strings to NULL.
- Validate maps and arrays.
- Assign extraction status.
```

### Phase 9 — Write Delta output

```text
- Write final rows into datasheet_extraction_results.
- Use MERGE or delete+insert by doc_id to avoid duplicates.
```

### Phase 10 — Quality gate

```text
- Run first-batch quality checks.
- Stop full processing if most fields are NULL.
- Improve schema/instructions before full run.
```

---

## 20. Definition of Done

The POC is done when:

```text
1. pdf_input.txt is used as the source of truth.
2. URLs are loaded from the Volume.
3. Plain URLs and HTML anchor URLs are handled.
4. URLs are deduplicated.
5. Control table is populated.
6. At least 1 to 5 PDFs are downloaded or staged into raw_pdfs.
7. PDFs are read using binaryFile.
8. ai_parse_document works on at least one PDF.
9. ai_extract works using fixed schema v2.
10. Results are written to datasheet_extraction_results.
11. Audit records exist for every major step.
12. Quality gate proves whether schema is strong enough before processing all URLs.
```

---

## 21. Speckit implementation instruction

Use this updated source input path everywhere:

```text
/Volumes/databricks_arrow_cata/main/pdf_ai/input_urls/pdf_input.txt
```

Do not use the old path:

```text
/Volumes/databricks_arrow_cata/main/pdf_ai/input_urls/pdf_urls.txt
```

The first implementation must focus on the loop:

```text
read pdf_input.txt
    ↓
extract URLs
    ↓
for each URL
    ↓
download/stage PDF
    ↓
parse PDF
    ↓
extract fixed schema
    ↓
write Delta result
    ↓
write audit row
```
