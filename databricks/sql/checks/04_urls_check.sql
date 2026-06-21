-- Phase URL-loader validation. Run inside Databricks via MCP.
-- Confirms control-table population, idempotency by cleaned_url, and metadata samples.

SELECT COUNT(*) AS total_control_rows
FROM databricks_arrow_cata.main.pdf_input_urls;

SELECT input_status, COUNT(*) AS row_count
FROM databricks_arrow_cata.main.pdf_input_urls
GROUP BY input_status
ORDER BY input_status;

SELECT COUNT(*) AS duplicate_new_cleaned_url_groups
FROM (
  SELECT cleaned_url
  FROM databricks_arrow_cata.main.pdf_input_urls
  WHERE input_status = 'NEW' AND cleaned_url IS NOT NULL
  GROUP BY cleaned_url
  HAVING COUNT(*) > 1
);

SELECT
  doc_id,
  source_url,
  cleaned_url,
  source_file_name,
  supplier_folder,
  url_part_hint,
  input_status
FROM databricks_arrow_cata.main.pdf_input_urls
ORDER BY doc_id DESC
LIMIT 20;

SELECT step_name, status, COUNT(*) AS audit_rows
FROM databricks_arrow_cata.main.pdf_extraction_audit
WHERE step_name IN ('READ_INPUT_FILE', 'CLEAN_URLS', 'INSERT_CONTROL_TABLE')
GROUP BY step_name, status
ORDER BY step_name, status;
