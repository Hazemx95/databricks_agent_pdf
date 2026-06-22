-- Phase 8 AI extraction validation. Run inside Databricks via MCP.
-- Confirms ai_extract ran on parsed_content for the small parsed POC batch only.
-- This check does not validate/normalize JSON, write final results, or run the quality gate.

SELECT COUNT(*) AS parsed_rows_available
FROM databricks_arrow_cata.main.pdf_parsed_documents
WHERE parse_status = 'PARSED';

SELECT
  COUNT(*) AS debug_rows_written,
  SUM(CASE WHEN extraction_attempt_status = 'OK' THEN 1 ELSE 0 END) AS successful_ai_extract_attempts,
  SUM(CASE WHEN extraction_attempt_status = 'ERROR' THEN 1 ELSE 0 END) AS failed_ai_extract_attempts
FROM read_files(
  '/Volumes/databricks_arrow_cata/main/pdf_ai/debug/phase8_ai_extract_debug',
  format => 'json'
);

SELECT
  doc_id,
  source_file_name,
  substr(raw_extraction_json, 1, 1200) AS raw_extraction_json_sample,
  fixed_schema_fields_present,
  extraction_attempt_status,
  get_json_object(raw_extraction_json, '$.error_message') AS extraction_error
FROM read_files(
  '/Volumes/databricks_arrow_cata/main/pdf_ai/debug/phase8_ai_extract_debug',
  format => 'json'
)
ORDER BY doc_id
LIMIT 3;

SELECT
  doc_id,
  source_file_name,
  part_number,
  manufacturer,
  description,
  extraction_confidence
FROM read_files(
  '/Volumes/databricks_arrow_cata/main/pdf_ai/debug/phase8_ai_extract_debug',
  format => 'json'
)
ORDER BY doc_id
LIMIT 10;

SELECT
  doc_id,
  cleaned_url,
  step_name,
  status,
  error_message,
  retry_count,
  started_at,
  finished_at,
  duration_seconds,
  model_endpoint,
  created_at
FROM databricks_arrow_cata.main.pdf_extraction_audit
WHERE step_name = 'AI_EXTRACT'
ORDER BY created_at DESC
LIMIT 20;

SELECT COUNT(*) AS final_result_rows_written_so_far
FROM databricks_arrow_cata.main.datasheet_extraction_results;

SELECT COUNT(*) AS validation_or_quality_or_write_audit_rows
FROM databricks_arrow_cata.main.pdf_extraction_audit
WHERE step_name IN ('VALIDATE_JSON', 'WRITE_RESULT', 'QUALITY_GATE');
