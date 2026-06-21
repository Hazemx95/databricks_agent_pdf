-- Phase 3 table creation for the PDF fixed-schema AI extraction POC.
-- Source of truth: PLAN.md. Idempotent and safe to rerun.

CREATE TABLE IF NOT EXISTS databricks_arrow_cata.main.pdf_input_urls (
  doc_id            BIGINT GENERATED ALWAYS AS IDENTITY,
  source_url        STRING,
  cleaned_url       STRING,
  source_file_name  STRING,
  supplier_folder   STRING,
  url_part_hint     STRING,
  input_status      STRING,
  local_pdf_path    STRING,
  error_message     STRING,
  created_at        TIMESTAMP,
  updated_at        TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS databricks_arrow_cata.main.pdf_parsed_documents (
  doc_id          BIGINT,
  cleaned_url     STRING,
  local_pdf_path  STRING,
  parsed_content  STRING,
  parse_status    STRING,
  parse_error     STRING,
  parsed_at       TIMESTAMP
) USING DELTA;

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
  extraction_status            STRING,
  extraction_error             STRING,
  processed_at                 TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS databricks_arrow_cata.main.pdf_extraction_audit (
  audit_id          BIGINT GENERATED ALWAYS AS IDENTITY,
  doc_id            BIGINT,
  cleaned_url       STRING,
  step_name         STRING,
  status            STRING,
  error_message     STRING,
  retry_count       INT,
  started_at        TIMESTAMP,
  finished_at       TIMESTAMP,
  duration_seconds  DOUBLE,
  model_endpoint    STRING,
  created_at        TIMESTAMP
) USING DELTA;
