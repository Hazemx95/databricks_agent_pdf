-- Phase PDF acquisition validation. Run inside Databricks via MCP.
-- Confirms small-batch PDF download/manual staging only; no binary read, parse, AI extraction, or final writes.

SELECT input_status, COUNT(*) AS row_count
FROM databricks_arrow_cata.main.pdf_input_urls
GROUP BY input_status
ORDER BY input_status;

SELECT
  doc_id,
  cleaned_url,
  source_file_name,
  input_status,
  local_pdf_path,
  error_message
FROM databricks_arrow_cata.main.pdf_input_urls
ORDER BY updated_at DESC, doc_id
LIMIT 10;

SELECT COUNT(*) AS pdfs_available_for_next_phase
FROM databricks_arrow_cata.main.pdf_input_urls
WHERE input_status IN ('DOWNLOADED', 'MANUALLY_STAGED')
  AND local_pdf_path IS NOT NULL;

SELECT COUNT(*) AS download_failure_count
FROM databricks_arrow_cata.main.pdf_input_urls
WHERE input_status = 'DOWNLOAD_FAILED';

SELECT doc_id, cleaned_url, source_file_name, error_message
FROM databricks_arrow_cata.main.pdf_input_urls
WHERE input_status = 'DOWNLOAD_FAILED'
ORDER BY updated_at DESC, doc_id
LIMIT 10;

SELECT step_name, status, COUNT(*) AS audit_rows
FROM databricks_arrow_cata.main.pdf_extraction_audit
WHERE step_name IN ('DOWNLOAD_PDF', 'STAGE_PDF')
GROUP BY step_name, status
ORDER BY step_name, status;

SELECT COUNT(*) AS binary_read_audit_rows
FROM databricks_arrow_cata.main.pdf_extraction_audit
WHERE step_name = 'READ_BINARY';

SELECT COUNT(*) AS parse_or_ai_audit_rows
FROM databricks_arrow_cata.main.pdf_extraction_audit
WHERE step_name IN ('PARSE_DOCUMENT', 'AI_EXTRACT');
