-- Data contract: Delta tables for the PDF fixed-schema AI extraction POC.
-- Catalog/schema/columns/types/statuses are verbatim from PLAN.md (§7, §11, §14, §16).
-- Idempotent: safe to run repeatedly. Run inside the Databricks workspace (Principle I, V).

-- Optional safety: ensure schema exists (catalog/volume assumed pre-provisioned per PLAN.md).
-- CREATE SCHEMA IF NOT EXISTS databricks_arrow_cata.main;

-- ---------------------------------------------------------------------------
-- 1. Control table: one row per cleaned PDF URL from pdf_input.txt
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS databricks_arrow_cata.main.pdf_input_urls (
  doc_id            BIGINT GENERATED ALWAYS AS IDENTITY,
  source_url        STRING,
  cleaned_url       STRING,
  source_file_name  STRING,
  supplier_folder   STRING,
  url_part_hint     STRING,
  input_status      STRING,   -- NEW | SKIPPED_DUPLICATE | INVALID_URL | DOWNLOADING |
                              -- DOWNLOADED | DOWNLOAD_FAILED | MANUALLY_STAGED
  local_pdf_path    STRING,
  error_message     STRING,
  created_at        TIMESTAMP,
  updated_at        TIMESTAMP
) USING DELTA;

-- ---------------------------------------------------------------------------
-- 2. Parsed documents: ai_parse_document output / status
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS databricks_arrow_cata.main.pdf_parsed_documents (
  doc_id          BIGINT,
  cleaned_url     STRING,
  local_pdf_path  STRING,
  parsed_content  STRING,    -- may be VARIANT depending on parser output handling
  parse_status    STRING,    -- PARSED | PARSE_FAILED | EMPTY_DOCUMENT
  parse_error     STRING,
  parsed_at       TIMESTAMP
) USING DELTA;

-- ---------------------------------------------------------------------------
-- 3. Final results: fixed schema v2 + provenance/control
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS databricks_arrow_cata.main.datasheet_extraction_results (
  doc_id                       BIGINT,
  source_url                   STRING,
  cleaned_url                  STRING,
  source_file_name             STRING,
  supplier_folder              STRING,
  url_part_hint                STRING,
  local_pdf_path               STRING,

  document_title               STRING,
  document_type                STRING,
  part_number                  STRING,
  manufacturer                 STRING,
  description                  STRING,
  product_family               STRING,
  product_category             STRING,

  part_number_candidates       ARRAY<STRING>,
  manufacturer_candidates      ARRAY<STRING>,

  electrical_specifications    MAP<STRING, STRING>,
  mechanical_specifications    MAP<STRING, STRING>,
  environmental_specifications MAP<STRING, STRING>,
  compliance_specifications    MAP<STRING, STRING>,
  general_specifications       MAP<STRING, STRING>,

  extraction_confidence        DOUBLE,
  extraction_notes             STRING,
  raw_extraction_json          STRING,
  schema_version               STRING,
  extraction_status            STRING,   -- SUCCESS | PARTIAL_SUCCESS | VALIDATION_FAILED | FAILED
  extraction_error             STRING,
  processed_at                 TIMESTAMP
) USING DELTA;

-- ---------------------------------------------------------------------------
-- 4. Audit: one row per executed step per document
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS databricks_arrow_cata.main.pdf_extraction_audit (
  audit_id          BIGINT GENERATED ALWAYS AS IDENTITY,
  doc_id            BIGINT,
  cleaned_url       STRING,
  step_name         STRING,   -- READ_INPUT_FILE | CLEAN_URLS | INSERT_CONTROL_TABLE |
                              -- DOWNLOAD_PDF | STAGE_PDF | READ_BINARY | PARSE_DOCUMENT |
                              -- AI_EXTRACT | VALIDATE_JSON | WRITE_RESULT | QUALITY_GATE
  status            STRING,
  error_message     STRING,
  retry_count       INT,
  started_at        TIMESTAMP,
  finished_at       TIMESTAMP,
  duration_seconds  DOUBLE,
  model_endpoint    STRING,
  created_at        TIMESTAMP
) USING DELTA;
