-- Phase parse-document validation. Run inside Databricks via MCP.
-- Confirms available PDFs were parsed into pdf_parsed_documents only.
-- This check does not run ai_extract, validate AI JSON, or write final results.

SELECT COUNT(*) AS available_pdfs_selected_for_parsing
FROM databricks_arrow_cata.main.pdf_input_urls
WHERE input_status IN ('DOWNLOADED', 'MANUALLY_STAGED')
  AND local_pdf_path IS NOT NULL;

SELECT COUNT(*) AS parsed_document_rows_written
FROM databricks_arrow_cata.main.pdf_parsed_documents;

SELECT parse_status, COUNT(*) AS row_count
FROM databricks_arrow_cata.main.pdf_parsed_documents
GROUP BY parse_status
ORDER BY parse_status;

SELECT
  doc_id,
  cleaned_url,
  local_pdf_path,
  parse_status,
  parse_error,
  parsed_at
FROM databricks_arrow_cata.main.pdf_parsed_documents
ORDER BY parsed_at DESC, doc_id
LIMIT 20;

SELECT
  doc_id,
  cleaned_url,
  local_pdf_path,
  parse_status,
  parse_error,
  parsed_at
FROM databricks_arrow_cata.main.pdf_parsed_documents
WHERE parse_status = 'PARSE_FAILED'
ORDER BY parsed_at DESC, doc_id
LIMIT 20;

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
WHERE step_name = 'PARSE_DOCUMENT'
ORDER BY created_at DESC
LIMIT 20;

SELECT COUNT(*) AS final_extraction_result_rows_written
FROM databricks_arrow_cata.main.datasheet_extraction_results;

SELECT COUNT(*) AS ai_extract_or_validation_or_result_audit_rows
FROM databricks_arrow_cata.main.pdf_extraction_audit
WHERE step_name IN ('AI_EXTRACT', 'VALIDATE_JSON', 'WRITE_RESULT');
