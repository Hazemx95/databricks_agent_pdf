-- Phase 9 final result validation. Run inside Databricks via MCP.
-- Confirms validation/normalization and idempotent final writes for the small POC batch only.
-- This check intentionally does not run the Phase 10 quality gate.

SELECT COUNT(*) AS raw_extraction_rows_used_as_input
FROM read_files(
  '/Volumes/databricks_arrow_cata/main/pdf_ai/debug/phase8_ai_extract_debug',
  format => 'json'
);

SELECT COUNT(*) AS final_result_rows_written
FROM databricks_arrow_cata.main.datasheet_extraction_results;

SELECT status_value AS extraction_status, COALESCE(result_counts.row_count, 0) AS row_count
FROM (
  SELECT 'SUCCESS' AS status_value
  UNION ALL SELECT 'PARTIAL_SUCCESS'
  UNION ALL SELECT 'VALIDATION_FAILED'
  UNION ALL SELECT 'FAILED'
) AS statuses
LEFT JOIN (
  SELECT extraction_status, COUNT(*) AS row_count
  FROM databricks_arrow_cata.main.datasheet_extraction_results
  GROUP BY extraction_status
) AS result_counts
ON statuses.status_value = result_counts.extraction_status
ORDER BY extraction_status;

SELECT
  doc_id,
  source_file_name,
  part_number,
  manufacturer,
  description,
  extraction_confidence,
  extraction_status,
  extraction_error,
  processed_at
FROM databricks_arrow_cata.main.datasheet_extraction_results
ORDER BY doc_id
LIMIT 10;

SELECT
  SUM(CASE WHEN part_number IS NULL THEN 1 ELSE 0 END) AS part_number_null_count,
  SUM(CASE WHEN manufacturer IS NULL THEN 1 ELSE 0 END) AS manufacturer_null_count,
  SUM(CASE WHEN description IS NULL THEN 1 ELSE 0 END) AS description_null_count
FROM databricks_arrow_cata.main.datasheet_extraction_results;

SELECT
  doc_id,
  source_file_name,
  part_number,
  manufacturer,
  description,
  document_title,
  extraction_status,
  extraction_error
FROM databricks_arrow_cata.main.datasheet_extraction_results
WHERE part_number IS NULL
  AND manufacturer IS NULL
  AND description IS NULL
  AND document_title IS NULL
ORDER BY doc_id;

SELECT
  COUNT(*) AS final_rows,
  SUM(CASE WHEN raw_extraction_json IS NOT NULL AND length(raw_extraction_json) > 0 THEN 1 ELSE 0 END) AS rows_with_raw_extraction_json,
  SUM(CASE WHEN raw_extraction_json IS NULL OR length(raw_extraction_json) = 0 THEN 1 ELSE 0 END) AS rows_missing_raw_extraction_json
FROM databricks_arrow_cata.main.datasheet_extraction_results;

SELECT doc_id, COUNT(*) AS row_count
FROM databricks_arrow_cata.main.datasheet_extraction_results
GROUP BY doc_id
HAVING COUNT(*) > 1;

SELECT
  step_name,
  status,
  COUNT(*) AS audit_rows,
  MIN(started_at) AS first_started_at,
  MAX(finished_at) AS last_finished_at
FROM databricks_arrow_cata.main.pdf_extraction_audit
WHERE step_name IN ('VALIDATE_JSON', 'WRITE_RESULT')
GROUP BY step_name, status
ORDER BY step_name, status;

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
  created_at
FROM databricks_arrow_cata.main.pdf_extraction_audit
WHERE step_name IN ('VALIDATE_JSON', 'WRITE_RESULT')
ORDER BY created_at DESC, doc_id
LIMIT 40;

SELECT COUNT(*) AS quality_gate_audit_rows_after_phase9
FROM databricks_arrow_cata.main.pdf_extraction_audit
WHERE step_name = 'QUALITY_GATE';
