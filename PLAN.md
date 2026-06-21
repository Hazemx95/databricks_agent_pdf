# PLAN.md — Databricks PDF Scraping / Datasheet Extraction POC

## 1. Objective

Build a Databricks pipeline that receives a list of PDF URLs, downloads/stages the PDF files, parses each PDF, extracts fixed structured fields using AI, validates the result, and writes the final data into a Unity Catalog Delta table.

The first POC will use a **fixed datasheet extraction schema v2**. The schema must be wider than the first simple example because the provided URLs point to different document families and naming patterns, such as `ech_`, `ittca_`, `wdm_`, and `fackg_` documents. Some files may be datasheets, product pages, connector specifications, or manuals.

Core business fields:

```json
{
  "part_number": "string or null",
  "manufacturer": "string or null",
  "description": "string or null",
  "key_specifications": "map<string,string>"
}
```

Hardened fallback fields:

```json
{
  "document_title": "string or null",
  "document_type": "string or null",
  "product_family": "string or null",
  "product_category": "string or null",
  "part_number_candidates": "array<string>",
  "electrical_specifications": "map<string,string>",
  "mechanical_specifications": "map<string,string>",
  "environmental_specifications": "map<string,string>",
  "compliance_specifications": "map<string,string>",
  "extraction_confidence": "double",
  "extraction_notes": "string or null"
}
```

When a PDF does not contain one of these fields, the pipeline must still write the row with `NULL` values for missing scalar fields and empty arrays/maps for missing collection fields. The final Delta row must never disappear only because the AI could not identify a part number.

---

## 2. Problem Statement

We have many PDF URLs from SiliconExpert download paths, for example:

```text
http://download.siliconexpert.com/pdfs2/2026/2/2/1/12/56/6983423418/ech_/manual/skupage.242307.pdf
http://download.siliconexpert.com/pdfs2/2026/2/2/0/22/48/9888444784/fackg_/manual/160522datasheet.pdf
http://download.siliconexpert.com/pdfs2/2026/2/2/5/45/32/5933908512/ittca_/manual/1122882604spec.pdf
```

The PDFs do not all have the same visual layout. Some are datasheets, some are product specification files, and some may have different naming/layout formats.

The required output must be fixed and queryable in Delta/Unity Catalog.

### 2.1 Initial URL Inventory to Store in Volume

For the first POC, the provided URL list must be stored as a text file in the Unity Catalog Volume:

```text
/Volumes/main/pdf_ai/input_urls/pdf_urls.txt
```

Each URL should be stored on a separate line. The list currently contains **40 PDF URLs**.

Observed URL families from the provided list:

| URL family folder | Count | Example file pattern | Expected document style |
|---|---:|---|---|
| `ech_` | 29 | `skupage.*.pdf` | SKU/product page or component page |
| `ittca_` | 7 | `ca*spec.pdf`, `mdm*spec.pdf` | connector specification files |
| `wdm_` | 3 | `*en.pdffilenameutf8*.pdf` | manual/datasheet naming pattern |
| `fackg_` | 1 | `*datasheet.pdf` | datasheet |

Full initial URL sample:

```text
http://download.siliconexpert.com/pdfs2/2026/2/2/1/12/56/6983423418/ech_/manual/skupage.242307.pdf
http://download.siliconexpert.com/pdfs2/2026/2/2/1/11/16/128316496/ech_/manual/skupage.108345.pdf
http://download.siliconexpert.com/pdfs2/2026/2/2/1/8/58/558961208/ech_/manual/skupage.093461.pdf
http://download.siliconexpert.com/pdfs2/2026/2/2/0/23/29/7158290520/ech_/manual/skupage.138988.pdf
http://download.siliconexpert.com/pdfs2/2026/2/2/0/22/29/1879365634/ech_/manual/skupage.199028.pdf
http://download.siliconexpert.com/pdfs2/2026/2/2/0/21/21/9868429619/ech_/manual/skupage.284166.pdf
http://download.siliconexpert.com/pdfs2/2026/2/2/0/22/48/9888444784/fackg_/manual/160522datasheet.pdf
http://download.siliconexpert.com/pdfs2/2026/2/2/0/22/18/189270838/ech_/manual/skupage.143231.pdf
http://download.siliconexpert.com/pdfs2/2026/2/2/2/9/44/40788429/wdm_/manual/1462600000en.pdffilenameutf81462600000en.pdf
http://download.siliconexpert.com/pdfs2/2026/2/2/5/45/32/5933908512/ittca_/manual/1122882604spec.pdf
http://download.siliconexpert.com/pdfs2/2026/2/2/5/45/27/488712293/ittca_/manual/ca02come14s5pxspec.pdf
http://download.siliconexpert.com/pdfs2/2026/2/2/5/46/39/7809096426/ittca_/manual/ca3102e183pf80spec.pdf
http://download.siliconexpert.com/pdfs2/2026/2/2/5/46/45/1359995313/ittca_/manual/ca3101r181pf42spec.pdf
http://download.siliconexpert.com/pdfs2/2026/2/2/5/45/7/46761657/ittca_/manual/mdm9phc38kspec.pdf
http://download.siliconexpert.com/pdfs2/2026/2/2/0/55/53/4382297831/ech_/manual/skupage.197625.pdf
http://download.siliconexpert.com/pdfs2/2026/2/2/0/57/19/7498938670/ech_/manual/skupage.102456.pdf
http://download.siliconexpert.com/pdfs2/2026/2/2/1/6/43/8627159323/ech_/manual/skupage.111297.pdf
http://download.siliconexpert.com/pdfs2/2026/2/2/1/7/37/3765009487/ech_/manual/skupage.bch21sc4.pdf
http://download.siliconexpert.com/pdfs2/2026/2/2/1/4/49/597760783/ech_/manual/skupage.53784112.pdf
http://download.siliconexpert.com/pdfs2/2026/2/2/1/6/13/7078677930/ech_/manual/skupage.090931.pdf
http://download.siliconexpert.com/pdfs2/2026/2/2/1/7/30/67318016/ech_/manual/skupage.278712.pdf
http://download.siliconexpert.com/pdfs2/2026/2/2/1/6/1/9688306241/ech_/manual/skupage.xtar007b21rd001.pdf
http://download.siliconexpert.com/pdfs2/2026/2/2/1/5/13/7921480331/ech_/manual/skupage.178858.pdf
http://download.siliconexpert.com/pdfs2/2026/2/2/1/6/38/7615242454/ech_/manual/skupage.271452.pdf
http://download.siliconexpert.com/pdfs2/2026/2/2/1/5/47/3512785933/ech_/manual/skupage.159111.pdf
http://download.siliconexpert.com/pdfs2/2026/2/2/1/5/38/998043816/ech_/manual/skupage.111910.pdf
http://download.siliconexpert.com/pdfs2/2026/2/2/1/4/53/6267593178/ech_/manual/skupage.231980.pdf
http://download.siliconexpert.com/pdfs2/2026/2/2/1/4/22/2066420664/ech_/manual/skupage.183769.pdf
http://download.siliconexpert.com/pdfs2/2026/2/2/1/6/49/1476367505/ech_/manual/skupage.ecx77d1tjbj.pdf
http://download.siliconexpert.com/pdfs2/2026/2/2/1/6/15/771188309/ech_/manual/skupage.evlla13lw1unv34.pdf
http://download.siliconexpert.com/pdfs2/2026/2/2/1/7/37/6797120909/ech_/manual/skupage.114608.pdf
http://download.siliconexpert.com/pdfs2/2026/2/2/1/6/9/9667892755/ech_/manual/skupage.132706.pdf
http://download.siliconexpert.com/pdfs2/2026/2/2/0/35/28/4781298481/ittca_/manual/mdm37phc26pspec.pdf
http://download.siliconexpert.com/pdfs2/2026/2/2/3/19/36/4059161730/ittca_/manual/ca3100f2815szspec.pdf
http://download.siliconexpert.com/pdfs2/2026/2/2/0/38/47/4856104232/ech_/manual/skupage.109723.pdf
http://download.siliconexpert.com/pdfs2/2026/2/2/0/38/17/1725248180/ech_/manual/skupage.284746.pdf
http://download.siliconexpert.com/pdfs2/2026/2/2/1/0/25/8918225403/ech_/manual/skupage.m22mwrsms7.pdf
http://download.siliconexpert.com/pdfs2/2026/2/2/5/48/34/7849622473/wdm_/manual/1020190000en.pdffilenameutf81020190000en.pdf
http://download.siliconexpert.com/pdfs2/2026/2/2/5/50/31/9367801519/wdm_/manual/9529200000en.pdffilenameutf89529200000en.pdf
http://download.siliconexpert.com/pdfs2/2026/2/2/0/42/14/6903097406/ech_/manual/skupage.m22pvt45pgb99.pdf
```

### 2.2 Static Schema Hardening Review

The first schema example was useful, but it is too small for this URL sample. If the extractor only asks for `part_number`, `manufacturer`, `description`, and `key_specifications`, some documents may return mostly null because:

1. Some files look like **SKU pages**, not classic datasheets.
2. Some files are **connector specification** PDFs where the part number may be embedded in a filename or drawing title.
3. Some files may use **ordering number**, **catalog number**, **series**, **type**, or **model** instead of the exact words `part number`.
4. Some PDFs may not clearly show manufacturer on the first page.
5. Some filenames contain valuable hints, such as `ca3102e183pf80spec.pdf`, `mdm37phc26pspec.pdf`, or `1020190000en...pdf`.

Therefore, the fixed schema must be **static but wide enough**. It should include:

- Core extracted business fields.
- Fallback/candidate fields.
- Document classification fields.
- Specification groups.
- URL/file metadata fields.
- Confidence and notes.

Important rule:

```text
Do not use filename-derived values as final AI-confirmed values unless the PDF content supports them.
Store filename-derived hints separately in metadata columns.
```

This avoids the situation where every extracted business field is null while still preserving useful traceability and candidates for review.

---

## 3. POC Environment and Workspace Access

### Databricks Environment

This POC will be developed on a free Databricks workspace/environment, so the first implementation must stay small and controlled.

Important constraints for the first POC:

- Start with a small number of PDFs before scaling.
- Avoid expensive large-batch AI extraction during initial testing.
- Keep intermediate tables/files so failed steps do not need to be repeated from zero.
- Confirm which Databricks AI features are available in the free workspace before final implementation.
- If a specific AI function or model endpoint is not available in the free workspace, keep the architecture the same and replace the extraction layer with the available Databricks model endpoint or approved LLM API later.

### MCP Server Access

Development will use an MCP server to connect the local development environment / Speckit workflow to the Databricks workspace.

The Databricks workspace connection configuration will be stored locally in:

```text
~/.databrickscfg
```

The `.databrickscfg` file must contain the Databricks host and authentication profile used by the MCP server.

Example shape:

```ini
[DEFAULT]
host = https://<databricks-workspace-url>
token = <databricks-token-or-auth-config>
```

Security rule:

```text
Never commit .databrickscfg to Git.
Never copy tokens into PLAN.md, README.md, notebooks, or source code.
```

Implementation target:

```text
Local Speckit / IDE
    ↓
MCP Server
    ↓
Databricks workspace using ~/.databrickscfg
    ↓
Databricks notebooks / jobs / Unity Catalog objects
```

---

## 4. High-Level Workflow

```text
Raw PDF URLs pasted/provided by user
    ↓
Save URL list file inside Unity Catalog Volume
    ↓
Read URL file from Volume
    ↓
Clean and deduplicate URLs
    ↓
Create input/control Delta table
    ↓
Download PDFs to Unity Catalog Volume
    ↓
Read PDF files as binary
    ↓
Parse PDFs with document parser
    ↓
Extract fields using AI with fixed schema
    ↓
Validate and normalize JSON
    ↓
Write structured rows to Delta table
    ↓
Write audit/error records
```

---

## 5. Scope

### In Scope

- Accept a pasted/provided list of PDF URLs for the first POC.
- Store the URL list as a file inside a Unity Catalog Volume before loading it into the control table.
- Clean malformed input such as HTML anchor tags.
- Deduplicate URLs.
- Download PDF files to a Unity Catalog Volume.
- Read staged PDFs as binary files.
- Parse PDF content.
- Extract the fixed schema v2:
  - `document_title`
  - `document_type`
  - `part_number`
  - `part_number_candidates`
  - `manufacturer`
  - `description`
  - grouped specification maps
- Store successful extraction results in a Unity Catalog Delta table.
- Store success/failure details in an audit Delta table.
- Keep raw JSON response for traceability.
- Allow missing fields as `NULL`.
- Support retry for failed files.

### Out of Scope for First POC

- No complex UI.
- No human review screen.
- No CDC/incremental document versioning.
- No dynamic schema generation per manufacturer.
- No advanced RAG/Genie integration.
- No real-time streaming.
- No enrichment from external product databases.
- No automatic correction by business users.

---

## 6. Recommended POC Architecture

### Catalog / Schema

```text
Catalog: main
Schema: pdf_ai
```

### Unity Catalog Volumes

```text
/Volumes/main/pdf_ai/input_urls/
/Volumes/main/pdf_ai/raw_pdfs/
```

Usage:

```text
/Volumes/main/pdf_ai/input_urls/
    Stores the pasted/provided PDF URL file, for example pdf_urls.txt or pdf_urls.csv.

/Volumes/main/pdf_ai/raw_pdfs/
    Stores downloaded PDF files before parsing.
```

Recommended first input file:

```text
/Volumes/main/pdf_ai/input_urls/pdf_urls.txt
```

The POC should not depend on URLs being hardcoded inside the notebook. The notebook should read the URL file from the Volume, clean it, and then insert the valid rows into the input control table.

---

## 7. Delta Tables

### 7.1 Input Control Table

Table:

```text
main.pdf_ai.pdf_input_urls
```

Purpose:

Stores every URL that must be processed.

Schema:

```sql
CREATE TABLE IF NOT EXISTS main.pdf_ai.pdf_input_urls (
    doc_id BIGINT GENERATED ALWAYS AS IDENTITY,
    source_url STRING,
    cleaned_url STRING,
    source_file_name STRING,
    supplier_folder STRING,
    url_part_hint STRING,
    local_pdf_path STRING,
    input_status STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING DELTA;
```

Recommended `input_status` values:

```text
NEW
DOWNLOADED
DOWNLOAD_FAILED
SKIPPED_DUPLICATE
INVALID_URL
```

---

### 7.2 Parsed Document Table

Table:

```text
main.pdf_ai.pdf_parsed_documents
```

Purpose:

Stores parsing result or extracted text/document representation.

Schema:

```sql
CREATE TABLE IF NOT EXISTS main.pdf_ai.pdf_parsed_documents (
    doc_id BIGINT,
    cleaned_url STRING,
    local_pdf_path STRING,
    parsed_content STRING,
    parse_status STRING,
    parse_error STRING,
    parsed_at TIMESTAMP
)
USING DELTA;
```

Recommended `parse_status` values:

```text
SUCCESS
FAILED
EMPTY_TEXT
UNSUPPORTED_FILE
```

---

### 7.3 Final Extraction Result Table

Table:

```text
main.pdf_ai.datasheet_extraction_results
```

Purpose:

Stores the fixed structured output.

Schema:

```sql
CREATE TABLE IF NOT EXISTS main.pdf_ai.datasheet_extraction_results (
    doc_id BIGINT,
    source_url STRING,
    cleaned_url STRING,
    source_file_name STRING,
    supplier_folder STRING,
    url_part_hint STRING,
    local_pdf_path STRING,

    document_title STRING,
    document_type STRING,
    manufacturer STRING,
    brand STRING,
    part_number STRING,
    part_number_candidates ARRAY<STRING>,
    product_family STRING,
    product_category STRING,
    description STRING,

    key_specifications MAP<STRING, STRING>,
    electrical_specifications MAP<STRING, STRING>,
    mechanical_specifications MAP<STRING, STRING>,
    environmental_specifications MAP<STRING, STRING>,
    compliance_specifications MAP<STRING, STRING>,

    extraction_confidence DOUBLE,
    extraction_notes STRING,
    missing_core_fields ARRAY<STRING>,

    raw_extraction_json STRING,
    schema_version STRING,
    extraction_status STRING,
    extraction_error STRING,

    processed_at TIMESTAMP
)
USING DELTA;
```

Recommended `extraction_status` values:

```text
SUCCESS
FAILED
PARTIAL_SUCCESS
VALIDATION_FAILED
```

---

### 7.4 Audit Table

Table:

```text
main.pdf_ai.pdf_extraction_audit
```

Purpose:

Tracks every step for debugging, retry, monitoring, and production support.

Schema:

```sql
CREATE TABLE IF NOT EXISTS main.pdf_ai.pdf_extraction_audit (
    audit_id BIGINT GENERATED ALWAYS AS IDENTITY,
    doc_id BIGINT,
    cleaned_url STRING,
    step_name STRING,
    status STRING,
    error_message STRING,
    retry_count INT,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    duration_seconds DOUBLE,
    model_endpoint STRING,
    created_at TIMESTAMP
)
USING DELTA;
```

---

## 8. Fixed Output Schema v2

The AI extraction output must always follow this fixed JSON structure. This schema is intentionally stronger than the first simple example to reduce the risk that all extracted values become null.

```json
{
  "document_title": null,
  "document_type": null,
  "manufacturer": null,
  "brand": null,
  "part_number": null,
  "part_number_candidates": [],
  "product_family": null,
  "product_category": null,
  "description": null,
  "key_specifications": {},
  "electrical_specifications": {},
  "mechanical_specifications": {},
  "environmental_specifications": {},
  "compliance_specifications": {},
  "extraction_confidence": 0.0,
  "extraction_notes": null
}
```

### Field Definitions

| Field | Type | Meaning | Null handling |
|---|---|---|---|
| `document_title` | STRING | Title found in the PDF, usually from first page/header | Null if no title found |
| `document_type` | STRING | One of: `datasheet`, `specification`, `manual`, `product_page`, `drawing`, `unknown` | `unknown` if unclear |
| `manufacturer` | STRING | Legal manufacturer name from document content | Null if not found |
| `brand` | STRING | Brand/trade name if different from manufacturer | Null if not found |
| `part_number` | STRING | Best single confirmed part/order/model number | Null if not confidently found |
| `part_number_candidates` | ARRAY<STRING> | Other possible part/order/model numbers found | Empty array if none |
| `product_family` | STRING | Product series/family, for example connector series | Null if not found |
| `product_category` | STRING | Product type/category, for example circular connector, terminal block, relay | Null if not found |
| `description` | STRING | Short product description from document | Null if not found |
| `key_specifications` | MAP<STRING,STRING> | General important specs | Empty map if none |
| `electrical_specifications` | MAP<STRING,STRING> | Voltage/current/power/resistance/frequency/rating values | Empty map if none |
| `mechanical_specifications` | MAP<STRING,STRING> | package, dimensions, shell size, contact count, material, mounting, termination | Empty map if none |
| `environmental_specifications` | MAP<STRING,STRING> | temperature, IP rating, humidity, vibration, operating environment | Empty map if none |
| `compliance_specifications` | MAP<STRING,STRING> | RoHS, REACH, UL, CE, approvals, lifecycle, safety standards | Empty map if none |
| `extraction_confidence` | DOUBLE | 0.0 to 1.0 estimated extraction confidence | 0.0 if extraction failed |
| `extraction_notes` | STRING | Short explanation when fields are missing or ambiguous | Null if not needed |

### Why This Schema Is Safer

The original example schema had only four fields. That can fail when the PDF is not a clean datasheet. The v2 schema is still fixed, but it gives the model more valid places to put useful information.

Examples:

```text
If the PDF is a connector specification:
- part_number may be found from drawing/order number.
- mechanical_specifications can capture shell size, contact arrangement, material, mounting, termination.

If the PDF is a SKU page:
- document_title, product_category, description, and part_number_candidates can still be extracted.

If the manufacturer is not explicit:
- manufacturer remains null.
- supplier_folder and url_part_hint are preserved from the URL metadata.
```

### AI Extraction Schema to Use

Use this schema in `ai_extract` or in the custom LLM prompt:

```json
{
  "document_title": {
    "type": "string",
    "description": "Main title of the PDF or product document"
  },
  "document_type": {
    "type": "string",
    "description": "Classify as datasheet, specification, manual, product_page, drawing, or unknown"
  },
  "manufacturer": {
    "type": "string",
    "description": "Manufacturer company name found in the document"
  },
  "brand": {
    "type": "string",
    "description": "Brand or trademark name if different from manufacturer"
  },
  "part_number": {
    "type": "string",
    "description": "Best single confirmed part number, ordering number, model number, or catalog number"
  },
  "part_number_candidates": {
    "type": "array",
    "items": { "type": "string" },
    "description": "All possible part numbers, model numbers, catalog numbers, or ordering codes found in the document"
  },
  "product_family": {
    "type": "string",
    "description": "Product series or family"
  },
  "product_category": {
    "type": "string",
    "description": "Product category such as circular connector, terminal block, relay, switch, cable, sensor, semiconductor, passive component"
  },
  "description": {
    "type": "string",
    "description": "Short product description from the PDF"
  },
  "key_specifications": {
    "type": "object",
    "additionalProperties": { "type": "string" },
    "description": "General important technical specifications as key-value pairs"
  },
  "electrical_specifications": {
    "type": "object",
    "additionalProperties": { "type": "string" },
    "description": "Electrical values such as voltage, current, power, resistance, capacitance, frequency, contact rating"
  },
  "mechanical_specifications": {
    "type": "object",
    "additionalProperties": { "type": "string" },
    "description": "Mechanical values such as package, dimensions, shell size, contacts, material, mounting type, termination type"
  },
  "environmental_specifications": {
    "type": "object",
    "additionalProperties": { "type": "string" },
    "description": "Environmental values such as operating temperature, storage temperature, IP rating, vibration, humidity"
  },
  "compliance_specifications": {
    "type": "object",
    "additionalProperties": { "type": "string" },
    "description": "Compliance and approvals such as RoHS, REACH, UL, CE, lifecycle, standards"
  },
  "extraction_confidence": {
    "type": "number",
    "description": "Confidence score from 0.0 to 1.0 based on evidence in the PDF"
  },
  "extraction_notes": {
    "type": "string",
    "description": "Short note explaining ambiguity or missing values"
  }
}
```

### Extraction Rules

1. Return the exact schema every time.
2. Do not invent values.
3. Use only PDF content for AI-confirmed values.
4. Return null for missing scalar fields.
5. Return empty arrays/maps for missing collection fields.
6. Keep units inside values, for example `5 V`, `2 A`, `-40°C to +85°C`.
7. If multiple possible part numbers exist, choose the best one as `part_number` and put all options in `part_number_candidates`.
8. Store URL/filename hints separately in metadata columns, not as confirmed extracted values.
9. If the document is not a datasheet, still classify it and extract whatever product-level information exists.

### Quality Gate for Schema

After running the first 10 PDFs, run this query:

```sql
SELECT
    COUNT(*) AS total_docs,
    SUM(CASE WHEN part_number IS NULL THEN 1 ELSE 0 END) AS null_part_number_count,
    SUM(CASE WHEN manufacturer IS NULL THEN 1 ELSE 0 END) AS null_manufacturer_count,
    SUM(CASE WHEN description IS NULL THEN 1 ELSE 0 END) AS null_description_count,
    SUM(CASE WHEN size(part_number_candidates) = 0 THEN 1 ELSE 0 END) AS no_candidates_count
FROM main.pdf_ai.datasheet_extraction_results;
```

If most rows have null core fields, inspect:

```sql
SELECT
    doc_id,
    source_file_name,
    document_title,
    document_type,
    part_number,
    part_number_candidates,
    manufacturer,
    extraction_notes,
    raw_extraction_json
FROM main.pdf_ai.datasheet_extraction_results
ORDER BY processed_at DESC;
```

Then improve the extraction instructions before scaling to all URLs.

---

## 9. AI Agent Definition

For this POC, the AI Agent is a controlled extraction component, not a chat bot.

### Agent Input

```json
{
  "doc_id": 1,
  "pdf_path": "/Volumes/main/pdf_ai/raw_pdfs/file.pdf",
  "parsed_document": "...",
  "output_schema": "datasheet_extraction_schema_v2"
}
```

### Agent Output

```json
{
  "document_title": "Example Product Specification",
  "document_type": "specification",
  "manufacturer": "Example Manufacturer",
  "brand": null,
  "part_number": "ABC123",
  "part_number_candidates": ["ABC123", "ABC123-01"],
  "product_family": "Example Series",
  "product_category": "connector",
  "description": "Example product description",
  "key_specifications": {
    "Package": "SMD"
  },
  "electrical_specifications": {
    "Voltage": "5V"
  },
  "mechanical_specifications": {},
  "environmental_specifications": {},
  "compliance_specifications": {},
  "extraction_confidence": 0.85,
  "extraction_notes": null
}
```

### Agent Responsibilities

- Read parsed PDF content.
- Identify product-level fields.
- Return only the required schema.
- Return null for missing fields.
- Avoid hallucination.
- Keep raw extraction response for audit.
- Mark failed/invalid output for retry.

---

## 10. LLM / AI Options

### Preferred Option for POC

Use Databricks-native AI document functions:

```text
read_files / binaryFile
    ↓
ai_parse_document
    ↓
ai_extract
```

This is the simplest path for first implementation.

### Alternative Option

Use `ai_query` against a Databricks Model Serving endpoint when more prompt/model control is required.

### Later Advanced Option

Build a custom Python agent using LangChain/LangGraph only if the workflow needs:

- document classification
- dynamic schema selection
- retries with reasoning
- product enrichment
- external lookup tools
- human review workflow
- comparison against existing Unity Catalog tables

For the first POC, avoid LangChain unless there is a clear requirement.

---

## 11. Implementation Phases for Speckit

## Phase 0 — Project Setup

### Goal

Create the repository/notebook structure and environment configuration.

### Deliverables

```text
src/
  config.py
  url_cleaner.py
  pdf_downloader.py
  pdf_parser.py
  extractor.py
  validator.py
  delta_writer.py

notebooks/
  01_create_tables.py
  02_load_input_urls.py
  03_download_pdfs.py
  04_parse_pdfs.py
  05_extract_fields.py
  06_write_results.py

tests/
  test_url_cleaner.py
  test_validator.py

PLAN.md
README.md
```

### Acceptance Criteria

- Project structure exists.
- Config values are not hardcoded except local development configuration.
- Unity Catalog names are configurable.
- MCP server can connect to the Databricks workspace using the local `~/.databrickscfg` profile.
- `.databrickscfg` is not committed to Git.
- No secrets are committed.

---

## Phase 1 — Create Unity Catalog Objects

### Goal

Create catalog/schema/volume/tables required for the POC.

### Tasks

1. Create schema `main.pdf_ai`.
2. Create input URL Volume `main.pdf_ai.input_urls`.
3. Create raw PDF Volume `main.pdf_ai.raw_pdfs`.
4. Create input table.
5. Create parsed document table.
6. Create extraction result table.
7. Create audit table.

### Acceptance Criteria

- All Delta tables exist.
- Tables can be queried from Databricks SQL.
- Volume path is writable by the job/cluster.

---

## Phase 2 — Store and Load URL List from Volume

### Goal

Store the pasted/provided PDF URLs inside a Unity Catalog Volume, then read this file and load clean URLs into the input control Delta table.

### URL Volume Path

```text
/Volumes/main/pdf_ai/input_urls/pdf_urls.txt
```

The file can be created manually for the first POC by pasting all URLs into `pdf_urls.txt`, one URL per line.

The file may contain:

- Plain PDF URLs.
- HTML anchor tags such as `<a href="...">...</a>`.
- Empty lines.
- Duplicate URLs.

### Required Logic

```text
Read /Volumes/main/pdf_ai/input_urls/pdf_urls.txt
    ↓
Extract href URL when line contains HTML anchor tag
    ↓
Extract plain URL when line contains direct http/https PDF link
    ↓
Trim spaces and trailing punctuation
    ↓
Validate URL
    ↓
Remove duplicates
    ↓
Insert clean URLs into main.pdf_ai.pdf_input_urls
```

### Example PySpark Logic

```python
from pyspark.sql import functions as F

URL_FILE_PATH = "/Volumes/main/pdf_ai/input_urls/pdf_urls.txt"

raw_urls_df = (
    spark.read
    .text(URL_FILE_PATH)
    .withColumnRenamed("value", "raw_line")
)

cleaned_urls_df = (
    raw_urls_df
    .withColumn("raw_line", F.trim(F.col("raw_line")))
    .withColumn(
        "href_url",
        F.regexp_extract(F.col("raw_line"), 'href="([^"]+)"', 1)
    )
    .withColumn(
        "plain_url",
        F.regexp_extract(F.col("raw_line"), '(https?://[^\s<>"]+)', 1)
    )
    .withColumn(
        "cleaned_url",
        F.when(F.col("href_url") != "", F.col("href_url"))
         .otherwise(F.col("plain_url"))
    )
    .withColumn("cleaned_url", F.regexp_replace(F.col("cleaned_url"), '[,]+$', ''))
    .withColumn("source_file_name", F.regexp_extract(F.col("cleaned_url"), r'([^/]+)$', 1))
    .withColumn("supplier_folder", F.regexp_extract(F.col("cleaned_url"), r'/([^/]+)/manual/[^/]+$', 1))
    .withColumn("url_part_hint", F.regexp_replace(F.regexp_extract(F.col("cleaned_url"), r'([^/]+)$', 1), r'(?i)(^skupage\.|datasheet\.pdf$|spec\.pdf$|\.pdf.*$)', ''))
    .withColumn(
        "input_status",
        F.when(F.col("cleaned_url").rlike(r'(?i)^https?://.*\.pdf.*'), F.lit("NEW"))
         .otherwise(F.lit("INVALID_URL"))
    )
    .withColumn("created_at", F.current_timestamp())
    .withColumn("updated_at", F.current_timestamp())
)

valid_urls_df = (
    cleaned_urls_df
    .filter(F.col("input_status") == "NEW")
    .dropDuplicates(["cleaned_url"])
    .select(
        F.col("raw_line").alias("source_url"),
        "cleaned_url",
        "source_file_name",
        "supplier_folder",
        "url_part_hint",
        F.lit(None).cast("string").alias("local_pdf_path"),
        "input_status",
        "created_at",
        "updated_at"
    )
)

valid_urls_df.write.mode("append").insertInto("main.pdf_ai.pdf_input_urls")
```

### Example URL Cleaning Rules

```text
Input:
<a href="http://download.siliconexpert.com/.../file.pdf" target="_blank">...</a>

Output:
http://download.siliconexpert.com/.../file.pdf
```

### Acceptance Criteria

- URL file exists inside `/Volumes/main/pdf_ai/input_urls/`.
- Notebook reads URLs from the Volume, not from hardcoded notebook code.
- All valid URLs inserted into `main.pdf_ai.pdf_input_urls`.
- Duplicates are not processed twice.
- Invalid rows are marked or logged as `INVALID_URL`.

---

## Phase 3 — Download PDF Files

### Goal

Download each PDF URL to Unity Catalog Volume.

### Requirements

- Use streaming download.
- Set request timeout.
- Validate HTTP status code.
- Validate PDF file signature when possible.
- Save each PDF using deterministic file name:
  - hash of URL
  - original file name
- Update `local_pdf_path`.
- Mark status as `DOWNLOADED` or `DOWNLOAD_FAILED`.

### Suggested Local File Naming

```text
/Volumes/main/pdf_ai/raw_pdfs/{doc_id}_{url_hash}_{file_name}
```

### Acceptance Criteria

- Successfully downloaded PDFs exist in UC Volume.
- Failed URLs are logged with error message.
- Pipeline can continue even if some URLs fail.

---

## Phase 4 — Read PDFs as Binary

### Goal

Load staged PDFs into Spark as binary files.

### SQL Pattern

```sql
SELECT
  path AS local_pdf_path,
  content,
  length,
  modificationTime
FROM read_files(
  '/Volumes/main/pdf_ai/raw_pdfs/',
  format => 'binaryFile',
  fileNamePattern => '*.{pdf,PDF}'
);
```

### Acceptance Criteria

- Each downloaded PDF becomes one Spark row.
- Binary content is available for parsing.
- File path can be joined back to `pdf_input_urls`.

---

## Phase 5 — Parse PDFs

### Goal

Convert binary PDF content into parsed text/document structure.

### Preferred Pattern

```sql
SELECT
  local_pdf_path,
  ai_parse_document(content) AS parsed_document
FROM pdf_binary_files;
```

### Requirements

- Store parsed output or extracted text in `pdf_parsed_documents`.
- Capture parse failures.
- Capture empty text cases.
- Do not stop the full batch because one PDF fails.

### Acceptance Criteria

- Parsed document table contains one row per processed PDF.
- Failed documents have useful error messages.
- Successful documents are ready for extraction.

---

## Phase 6 — Extract Fixed Datasheet Fields

### Goal

Use AI to extract the fixed schema.

### Extraction Instructions

The prompt/instructions must tell the model:

```text
Extract product information from this PDF document.
Return only valid JSON.
Use the exact fixed schema v2 from section 8.
Do not invent values.
Use only values found in the PDF content.
If a scalar field does not exist, return null.
If a collection field does not exist, return an empty array or empty object.
The document may be a datasheet, product page, connector specification, drawing, or manual.
Treat words like part number, order number, ordering code, catalog number, model, type, and series as possible part-number evidence.
For specifications, include important electrical, mechanical, environmental, and compliance properties found in the document.
```

### Acceptance Criteria

- Output is valid JSON.
- Missing fields are null.
- No extra root-level fields are written into the final table.
- Raw JSON is stored for debugging.

---

## Phase 7 — Validate and Normalize JSON

### Goal

Protect the Delta table from invalid AI output.

### Validation Rules

- JSON must parse successfully.
- Required root keys from schema v2 must exist:
  - `document_title`
  - `document_type`
  - `manufacturer`
  - `brand`
  - `part_number`
  - `part_number_candidates`
  - `product_family`
  - `product_category`
  - `description`
  - `key_specifications`
  - `electrical_specifications`
  - `mechanical_specifications`
  - `environmental_specifications`
  - `compliance_specifications`
  - `extraction_confidence`
  - `extraction_notes`
- Scalar fields must be strings or null.
- `part_number_candidates` must be an array of strings.
- Specification fields must be map/object or empty map.
- All map keys and values must be converted to strings.
- Empty strings should be normalized to null.
- `missing_core_fields` should be calculated after extraction, not requested from the model.

### Acceptance Criteria

- Valid rows are written as `SUCCESS`.
- Partial rows with missing fields are written as `PARTIAL_SUCCESS`.
- Invalid JSON is marked `VALIDATION_FAILED`.
- Error reason is stored.

---

## Phase 8 — Write Final Delta Results

### Goal

Insert structured rows into the final Delta table.

### Write Mode

For first POC:

```text
append
```

### Recommended Idempotency Rule

Use `doc_id` as the processing key.

Before inserting new result for the same `doc_id`, either:

1. Delete old result for `doc_id`, then insert new result.
2. Or use MERGE to upsert latest result.

### Acceptance Criteria

- Result table contains one final row per processed PDF.
- Re-running the job does not create uncontrolled duplicates.
- Querying by `part_number` and `manufacturer` works.

---

## Phase 9 — Audit and Monitoring

### Goal

Make the pipeline supportable in production.

### Required Metrics

- Number of input URLs.
- Number of downloaded PDFs.
- Number of download failures.
- Number of parsed PDFs.
- Number of parse failures.
- Number of successful extractions.
- Number of validation failures.
- Total runtime.
- Average time per PDF.

### Acceptance Criteria

- Audit table has a record for every major step.
- Failed files can be identified and retried.
- Summary metrics can be displayed in notebook or dashboard.

---

## Phase 10 — Retry Failed PDFs

### Goal

Allow reprocessing only failed documents.

### Retry Criteria

Retry rows where:

```sql
input_status = 'DOWNLOAD_FAILED'
OR parse_status = 'FAILED'
OR extraction_status IN ('FAILED', 'VALIDATION_FAILED')
```

### Acceptance Criteria

- Retry job can process failed files only.
- Retry count is incremented.
- Latest error is saved.

---

## 12. Example Final Table Output

```text
doc_id: 1
source_url: http://download.siliconexpert.com/...
source_file_name: ca3102e183pf80spec.pdf
supplier_folder: ittca_
url_part_hint: ca3102e183pf80
document_title: CA3102E18-3P-F80 Specification
document_type: specification
part_number: CA3102E18-3P-F80
part_number_candidates: ["CA3102E18-3P-F80", "CA3102E18-3P"]
manufacturer: Example Manufacturer
description: Circular connector specification
product_category: circular connector
key_specifications: {"Series":"CA"}
electrical_specifications: {"Contact Rating":"value from PDF"}
mechanical_specifications: {"Shell Size":"18", "Contact Arrangement":"3P"}
extraction_status: SUCCESS
processed_at: 2026-06-21 10:00:00
```

When data is missing:

```text
doc_id: 2
source_url: http://download.siliconexpert.com/...
source_file_name: skupage.108345.pdf
supplier_folder: ech_
url_part_hint: 108345
document_title: Product page or SKU page title from PDF
document_type: product_page
part_number: NULL
part_number_candidates: ["108345"]
manufacturer: NULL
description: Connector accessory datasheet
product_category: connector accessory
key_specifications: {"Material":"Plastic"}
electrical_specifications: {}
mechanical_specifications: {"Material":"Plastic"}
extraction_status: PARTIAL_SUCCESS
processed_at: 2026-06-21 10:05:00
```

---

## 13. Production Concerns

### Network

The Databricks cluster/serverless environment must be able to access:

```text
http://download.siliconexpert.com
```

If the domain is internal or protected, configure network access before implementation.

### Security

- Do not hardcode secrets.
- Do not expose signed/private URLs in logs.
- Mask sensitive parameters.
- Use Unity Catalog permissions.
- Store raw PDFs only in approved volumes.
- Keep `.databrickscfg` local and outside Git.
- MCP server should use the configured Databricks profile only; do not paste workspace tokens into notebooks.

### Cost

AI parsing/extraction can have model inference cost or workspace feature limitations. Because the first implementation is on a free Databricks environment, the POC should start with a small sample before scaling to thousands of PDFs.

### Performance

For large batches:

- Download in controlled parallelism.
- Limit concurrent AI extraction calls.
- Process in batches.
- Store intermediate parse results.
- Retry only failed PDFs.
- Avoid re-parsing PDFs that already succeeded.

### Quality

AI extraction can be wrong if:

- PDF is scanned image.
- PDF has poor OCR quality.
- Part number is repeated many times.
- Manufacturer name is not explicit.
- The document is not a datasheet.
- Tables are split across pages.

The audit table and raw JSON are required for validation.

---

## 14. Definition of Done

The POC is complete when:

1. A list of PDF URLs is stored in `/Volumes/main/pdf_ai/input_urls/pdf_urls.txt` and loaded into `pdf_input_urls`.
2. PDFs are downloaded to UC Volume.
3. PDFs are read as binary files.
4. PDFs are parsed successfully where possible.
5. AI extracts the fixed schema v2 fields:
   - `document_title`
   - `document_type`
   - `part_number`
   - `part_number_candidates`
   - `manufacturer`
   - `description`
   - `key_specifications`
   - grouped specification maps
6. Missing scalar fields are stored as null, and missing arrays/maps are stored as empty arrays/maps.
7. Final results are written to:
   - `main.pdf_ai.datasheet_extraction_results`
8. Failures are written to:
   - `main.pdf_ai.pdf_extraction_audit`
9. The job can be re-run safely.
10. A summary query shows success/failure counts.

---

## 15. Suggested Speckit Command Flow

Use Speckit phases like this:

```text
/specify
Build Databricks PDF scraping pipeline in a free Databricks workspace. Use MCP server access through local ~/.databrickscfg. Store the PDF URL list inside a Unity Catalog Volume, read and clean URLs from the Volume, download PDFs, parse PDFs, extract fixed datasheet schema using AI, validate JSON, and write to Unity Catalog Delta tables.

/plan
Use PySpark + Databricks SQL + Unity Catalog Delta tables. Prefer Databricks AI document functions for parsing/extraction when available in the free workspace. Include MCP/.databrickscfg workspace access, URL file storage in Volume, audit, retry, and small-sample cost control.

/tasks
Generate implementation tasks by phase:
1. Create UC objects and Volumes
2. Configure MCP workspace access through ~/.databrickscfg
3. Store URL list file inside Volume
4. Load/clean URL input from Volume
5. Download PDFs to raw_pdfs Volume
6. Read binary files
7. Parse PDFs
8. Extract fixed schema
9. Validate JSON
10. Write Delta results
11. Audit and retry
```

---

## 16. First POC Success Query

```sql
SELECT
    extraction_status,
    COUNT(*) AS total_pdfs
FROM main.pdf_ai.datasheet_extraction_results
GROUP BY extraction_status;
```

Example inspection query:

```sql
SELECT
    doc_id,
    source_file_name,
    supplier_folder,
    url_part_hint,
    document_title,
    document_type,
    part_number,
    part_number_candidates,
    manufacturer,
    product_category,
    description,
    key_specifications,
    electrical_specifications,
    mechanical_specifications,
    extraction_status
FROM main.pdf_ai.datasheet_extraction_results
ORDER BY processed_at DESC;
```
