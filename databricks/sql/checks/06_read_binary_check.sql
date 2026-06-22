-- Phase binary-read validation. Run inside Databricks via MCP.
-- Confirms raw PDFs are read as binary and joined to the control table only.
-- This check does not call ai_parse_document, ai_extract, validate AI JSON, or write final results.

SELECT COUNT(*) AS pdf_files_found_in_raw_pdfs
FROM binaryFile.`/Volumes/databricks_arrow_cata/main/pdf_ai/raw_pdfs/`;

SELECT COUNT(*) AS available_control_table_rows
FROM databricks_arrow_cata.main.pdf_input_urls
WHERE input_status IN ('DOWNLOADED', 'MANUALLY_STAGED')
  AND local_pdf_path IS NOT NULL;

SELECT COUNT(*) AS binary_files_successfully_read
FROM binaryFile.`/Volumes/databricks_arrow_cata/main/pdf_ai/raw_pdfs/`;

WITH raw_binary_files AS (
  SELECT path, content, length, modificationTime
  FROM binaryFile.`/Volumes/databricks_arrow_cata/main/pdf_ai/raw_pdfs/`
),
available_control AS (
  SELECT doc_id, source_file_name, url_part_hint, input_status, local_pdf_path
  FROM databricks_arrow_cata.main.pdf_input_urls
  WHERE input_status IN ('DOWNLOADED', 'MANUALLY_STAGED')
    AND local_pdf_path IS NOT NULL
)
SELECT COUNT(DISTINCT c.doc_id) AS matched_pdfs
FROM raw_binary_files b
INNER JOIN available_control c
  ON b.path = c.local_pdf_path
  OR element_at(split(b.path, '/'), -1) = element_at(split(c.local_pdf_path, '/'), -1)
  OR element_at(split(b.path, '/'), -1) = c.source_file_name
  OR instr(element_at(split(b.path, '/'), -1), c.source_file_name) > 0
  OR (length(c.url_part_hint) >= 4 AND instr(element_at(split(b.path, '/'), -1), c.url_part_hint) > 0);

WITH raw_binary_files AS (
  SELECT path, content, length, modificationTime
  FROM binaryFile.`/Volumes/databricks_arrow_cata/main/pdf_ai/raw_pdfs/`
),
available_control AS (
  SELECT doc_id, source_file_name, url_part_hint, input_status, local_pdf_path
  FROM databricks_arrow_cata.main.pdf_input_urls
  WHERE input_status IN ('DOWNLOADED', 'MANUALLY_STAGED')
    AND local_pdf_path IS NOT NULL
)
SELECT
  c.doc_id,
  c.source_file_name,
  c.input_status,
  c.local_pdf_path,
  b.path AS binary_file_path,
  b.length AS file_size,
  b.modificationTime
FROM raw_binary_files b
INNER JOIN available_control c
  ON b.path = c.local_pdf_path
  OR element_at(split(b.path, '/'), -1) = element_at(split(c.local_pdf_path, '/'), -1)
  OR element_at(split(b.path, '/'), -1) = c.source_file_name
  OR instr(element_at(split(b.path, '/'), -1), c.source_file_name) > 0
  OR (length(c.url_part_hint) >= 4 AND instr(element_at(split(b.path, '/'), -1), c.url_part_hint) > 0)
ORDER BY c.doc_id
LIMIT 20;

WITH raw_binary_files AS (
  SELECT path, content, length, modificationTime
  FROM binaryFile.`/Volumes/databricks_arrow_cata/main/pdf_ai/raw_pdfs/`
),
available_control AS (
  SELECT doc_id, source_file_name, url_part_hint, input_status, local_pdf_path
  FROM databricks_arrow_cata.main.pdf_input_urls
  WHERE input_status IN ('DOWNLOADED', 'MANUALLY_STAGED')
    AND local_pdf_path IS NOT NULL
),
matched AS (
  SELECT DISTINCT b.path AS binary_file_path
  FROM raw_binary_files b
  INNER JOIN available_control c
    ON b.path = c.local_pdf_path
    OR element_at(split(b.path, '/'), -1) = element_at(split(c.local_pdf_path, '/'), -1)
    OR element_at(split(b.path, '/'), -1) = c.source_file_name
    OR instr(element_at(split(b.path, '/'), -1), c.source_file_name) > 0
    OR (length(c.url_part_hint) >= 4 AND instr(element_at(split(b.path, '/'), -1), c.url_part_hint) > 0)
)
SELECT b.path AS unmatched_pdf_file
FROM raw_binary_files b
LEFT ANTI JOIN matched m
  ON b.path = m.binary_file_path
ORDER BY unmatched_pdf_file;

SELECT path AS zero_byte_pdf_file, length AS file_size, modificationTime
FROM binaryFile.`/Volumes/databricks_arrow_cata/main/pdf_ai/raw_pdfs/`
WHERE length <= 0 OR length IS NULL
ORDER BY path;

SELECT
  doc_id,
  cleaned_url,
  status,
  error_message,
  started_at,
  finished_at,
  duration_seconds,
  created_at
FROM databricks_arrow_cata.main.pdf_extraction_audit
WHERE step_name = 'READ_BINARY'
ORDER BY created_at DESC
LIMIT 20;

SELECT COUNT(*) AS parse_or_ai_or_result_audit_rows
FROM databricks_arrow_cata.main.pdf_extraction_audit
WHERE step_name IN ('PARSE_DOCUMENT', 'AI_EXTRACT', 'VALIDATE_JSON', 'WRITE_RESULT');
