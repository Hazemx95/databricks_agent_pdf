-- Phase URL loader: pdf_input.txt -> databricks_arrow_cata.main.pdf_input_urls.
-- Run inside Databricks SQL. Stops before PDF download/binary/parse/AI extraction.

CREATE OR REPLACE TEMP VIEW phase_url_loader_raw AS
SELECT
  monotonically_increasing_id() AS row_number,
  trim(value) AS source_url
FROM read_files(
  '/Volumes/databricks_arrow_cata/main/pdf_ai/input_urls/pdf_input.txt',
  format => 'text'
)
WHERE trim(value) <> '';

INSERT INTO databricks_arrow_cata.main.pdf_extraction_audit (
  doc_id, cleaned_url, step_name, status, error_message, retry_count,
  started_at, finished_at, duration_seconds, model_endpoint, created_at
)
SELECT NULL, NULL, 'READ_INPUT_FILE', 'OK', concat('rows_read=', CAST(COUNT(*) AS STRING)),
       0, current_timestamp(), current_timestamp(), 0.0, NULL, current_timestamp()
FROM phase_url_loader_raw;

CREATE OR REPLACE TEMP VIEW phase_url_loader_candidates AS
SELECT
  row_number,
  source_url,
  CASE
    WHEN regexp_extract(source_url, '(?i)href\\s*=\\s*["'']([^"'']+)["'']', 1) <> ''
      THEN regexp_extract(source_url, '(?i)href\\s*=\\s*["'']([^"'']+)["'']', 1)
    ELSE regexp_extract(source_url, '(?i)https?://\\S+', 0)
  END AS candidate_url
FROM phase_url_loader_raw;

CREATE OR REPLACE TEMP VIEW phase_url_loader_cleaned AS
SELECT
  row_number,
  source_url,
  NULLIF(regexp_replace(trim(candidate_url), '[\\s,;\\)\\.\\(<>"\\]\\}]+$', ''), '') AS cleaned_url
FROM phase_url_loader_candidates;

CREATE OR REPLACE TEMP VIEW phase_url_loader_classified AS
SELECT
  row_number,
  source_url,
  cleaned_url,
  CASE
    WHEN cleaned_url RLIKE '(?i)^https?://[^/]+.+'
      AND (
        lower(cleaned_url) RLIKE '\\.pdf($|[?#])'
        OR lower(parse_url(cleaned_url, 'HOST')) LIKE '%siliconexpert.com%'
      )
      THEN 'NEW'
    ELSE 'INVALID_URL'
  END AS input_status
FROM phase_url_loader_cleaned;

CREATE OR REPLACE TEMP VIEW phase_url_loader_valid_ranked AS
SELECT
  *,
  row_number() OVER (PARTITION BY cleaned_url ORDER BY row_number) AS duplicate_rank
FROM phase_url_loader_classified
WHERE input_status = 'NEW';

CREATE OR REPLACE TEMP VIEW phase_url_loader_valid_unique AS
SELECT
  source_url,
  cleaned_url,
  regexp_extract(cleaned_url, '.*/([^/?#]+)(?:[?#].*)?$', 1) AS source_file_name,
  NULLIF(regexp_extract(cleaned_url, '/([^/]*_)/[^?#]*[^/]+(?:[?#].*)?$', 1), '') AS supplier_folder,
  regexp_replace(
    CASE
      WHEN lower(regexp_replace(regexp_extract(cleaned_url, '.*/([^/?#]+)(?:[?#].*)?$', 1), '(?i)\\.pdf$', '')) LIKE 'skupage.%'
        THEN regexp_extract(regexp_replace(regexp_extract(cleaned_url, '.*/([^/?#]+)(?:[?#].*)?$', 1), '(?i)\\.pdf$', ''), '(?i)^skupage\\.([^.]*)$', 1)
      ELSE regexp_replace(regexp_extract(cleaned_url, '.*/([^/?#]+)(?:[?#].*)?$', 1), '(?i)\\.pdf$', '')
    END,
    '(?i)(datasheet|spec)$',
    ''
  ) AS url_part_hint,
  'NEW' AS input_status,
  CAST(NULL AS STRING) AS local_pdf_path,
  CAST(NULL AS STRING) AS error_message
FROM phase_url_loader_valid_ranked
WHERE duplicate_rank = 1;

CREATE OR REPLACE TEMP VIEW phase_url_loader_invalid AS
SELECT
  source_url,
  cleaned_url,
  CAST(NULL AS STRING) AS source_file_name,
  CAST(NULL AS STRING) AS supplier_folder,
  CAST(NULL AS STRING) AS url_part_hint,
  'INVALID_URL' AS input_status,
  CAST(NULL AS STRING) AS local_pdf_path,
  'INVALID_URL' AS error_message
FROM phase_url_loader_classified
WHERE input_status = 'INVALID_URL';

INSERT INTO databricks_arrow_cata.main.pdf_extraction_audit (
  doc_id, cleaned_url, step_name, status, error_message, retry_count,
  started_at, finished_at, duration_seconds, model_endpoint, created_at
)
SELECT
  NULL, NULL, 'CLEAN_URLS', 'OK',
  to_json(named_struct(
    'urls_extracted', (SELECT COUNT(*) FROM phase_url_loader_cleaned WHERE cleaned_url IS NOT NULL),
    'valid_cleaned_urls', (SELECT COUNT(*) FROM phase_url_loader_valid_unique),
    'duplicate_input_rows_skipped', (SELECT COUNT(*) FROM phase_url_loader_valid_ranked WHERE duplicate_rank > 1),
    'invalid_rows', (SELECT COUNT(*) FROM phase_url_loader_invalid)
  )),
  0, current_timestamp(), current_timestamp(), 0.0, NULL, current_timestamp();

CREATE OR REPLACE TEMP VIEW phase_url_loader_valid_missing AS
SELECT v.*
FROM phase_url_loader_valid_unique v
LEFT ANTI JOIN (
  SELECT DISTINCT cleaned_url
  FROM databricks_arrow_cata.main.pdf_input_urls
  WHERE cleaned_url IS NOT NULL
) existing
ON v.cleaned_url = existing.cleaned_url;

CREATE OR REPLACE TEMP VIEW phase_url_loader_invalid_missing AS
SELECT i.*
FROM phase_url_loader_invalid i
LEFT ANTI JOIN (
  SELECT source_url, input_status
  FROM databricks_arrow_cata.main.pdf_input_urls
  WHERE input_status = 'INVALID_URL'
) existing
ON i.source_url = existing.source_url AND i.input_status = existing.input_status;

INSERT INTO databricks_arrow_cata.main.pdf_input_urls (
  source_url, cleaned_url, source_file_name, supplier_folder, url_part_hint,
  input_status, local_pdf_path, error_message, created_at, updated_at
)
SELECT
  source_url, cleaned_url, source_file_name, supplier_folder, url_part_hint,
  input_status, local_pdf_path, error_message, current_timestamp(), current_timestamp()
FROM phase_url_loader_valid_missing;

INSERT INTO databricks_arrow_cata.main.pdf_input_urls (
  source_url, cleaned_url, source_file_name, supplier_folder, url_part_hint,
  input_status, local_pdf_path, error_message, created_at, updated_at
)
SELECT
  source_url, cleaned_url, source_file_name, supplier_folder, url_part_hint,
  input_status, local_pdf_path, error_message, current_timestamp(), current_timestamp()
FROM phase_url_loader_invalid_missing;

INSERT INTO databricks_arrow_cata.main.pdf_extraction_audit (
  doc_id, cleaned_url, step_name, status, error_message, retry_count,
  started_at, finished_at, duration_seconds, model_endpoint, created_at
)
SELECT
  NULL, NULL, 'INSERT_CONTROL_TABLE', 'OK',
  to_json(named_struct(
    'valid_rows_inserted', (SELECT COUNT(*) FROM phase_url_loader_valid_missing),
    'invalid_rows_inserted', (SELECT COUNT(*) FROM phase_url_loader_invalid_missing),
    'already_existing_valid_urls_skipped', (
      (SELECT COUNT(*) FROM phase_url_loader_valid_unique) - (SELECT COUNT(*) FROM phase_url_loader_valid_missing)
    )
  )),
  0, current_timestamp(), current_timestamp(), 0.0, NULL, current_timestamp();

SELECT
  (SELECT COUNT(*) FROM phase_url_loader_raw) AS rows_read,
  (SELECT COUNT(*) FROM phase_url_loader_cleaned WHERE cleaned_url IS NOT NULL) AS urls_extracted,
  (SELECT COUNT(*) FROM phase_url_loader_valid_unique) AS valid_cleaned_urls,
  (
    (SELECT COUNT(*) FROM phase_url_loader_valid_ranked WHERE duplicate_rank > 1)
    + ((SELECT COUNT(*) FROM phase_url_loader_valid_unique) - (SELECT COUNT(*) FROM phase_url_loader_valid_missing))
  ) AS duplicate_urls_skipped,
  (SELECT COUNT(*) FROM phase_url_loader_invalid) AS invalid_rows,
  ((SELECT COUNT(*) FROM phase_url_loader_valid_missing) + (SELECT COUNT(*) FROM phase_url_loader_invalid_missing)) AS rows_inserted_into_pdf_input_urls,
  0 AS pdfs_downloaded,
  0 AS pdfs_read_as_binary,
  0 AS pdfs_parsed,
  0 AS ai_extractions_run;
