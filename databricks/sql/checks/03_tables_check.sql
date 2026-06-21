-- Phase 3 validation checks. Run inside Databricks through MCP.
-- Confirms required tables exist and are queryable without loading URLs or processing PDFs.

SHOW TABLES IN databricks_arrow_cata.main LIKE 'pdf_input_urls';
SHOW TABLES IN databricks_arrow_cata.main LIKE 'pdf_parsed_documents';
SHOW TABLES IN databricks_arrow_cata.main LIKE 'datasheet_extraction_results';
SHOW TABLES IN databricks_arrow_cata.main LIKE 'pdf_extraction_audit';

DESCRIBE TABLE databricks_arrow_cata.main.pdf_input_urls;
DESCRIBE TABLE databricks_arrow_cata.main.pdf_parsed_documents;
DESCRIBE TABLE databricks_arrow_cata.main.datasheet_extraction_results;
DESCRIBE TABLE databricks_arrow_cata.main.pdf_extraction_audit;

SELECT 'pdf_input_urls' AS table_name, COUNT(*) AS row_count
FROM databricks_arrow_cata.main.pdf_input_urls;

SELECT 'pdf_parsed_documents' AS table_name, COUNT(*) AS row_count
FROM databricks_arrow_cata.main.pdf_parsed_documents;

SELECT 'datasheet_extraction_results' AS table_name, COUNT(*) AS row_count
FROM databricks_arrow_cata.main.datasheet_extraction_results;

SELECT 'pdf_extraction_audit' AS table_name, COUNT(*) AS row_count
FROM databricks_arrow_cata.main.pdf_extraction_audit;
