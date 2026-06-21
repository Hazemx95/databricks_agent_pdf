# Contract: Audit Steps & Status Semantics

Every major step writes exactly one `pdf_extraction_audit` row per `doc_id` it processes
(Principle IX, PLAN.md §16). Batch-level steps (no single doc) write one row with `doc_id = NULL`.

| `step_name` | Phase | Scope | Typical `status` values | Sets `model_endpoint`? |
|-------------|-------|-------|-------------------------|------------------------|
| `READ_INPUT_FILE` | 3 | batch | OK / ERROR | no |
| `CLEAN_URLS` | 3 | batch | OK / ERROR | no |
| `INSERT_CONTROL_TABLE` | 3 | batch | OK / ERROR | no |
| `DOWNLOAD_PDF` | 4 | per doc | DOWNLOADED / DOWNLOAD_FAILED | no |
| `STAGE_PDF` | 4 | per doc | MANUALLY_STAGED / NO_MATCH | no |
| `READ_BINARY` | 5 | per doc | OK / ERROR | no |
| `PARSE_DOCUMENT` | 6 | per doc | PARSED / PARSE_FAILED / EMPTY_DOCUMENT | optional |
| `AI_EXTRACT` | 7 | per doc | OK / ERROR | **yes** |
| `VALIDATE_JSON` | 8 | per doc | SUCCESS / PARTIAL_SUCCESS / VALIDATION_FAILED / FAILED | no |
| `WRITE_RESULT` | 9 | per doc | OK / ERROR | no |
| `QUALITY_GATE` | 10 | batch | PASS / STOP | no |

## Required fields per audit row

- `step_name`, `status` — always set.
- `error_message` — set when status indicates failure; else NULL.
- `retry_count` — current retry attempt for the doc (0 on first pass).
- `started_at`, `finished_at`, `duration_seconds` — timing of the step.
- `model_endpoint` — set for `AI_EXTRACT` (the `ai_query` endpoint); optional for `PARSE_DOCUMENT`.
- `created_at` — audit row insert time.

## Invariants

1. **No silent steps**: every executed step in phases 3–10 produces an audit row. A reviewer can
   reconstruct each doc's full path from `pdf_extraction_audit` filtered by `doc_id`, ordered by
   `started_at`.
2. **Failures are recorded, not raised**: a per-doc failure writes an ERROR/FAILED audit row and the
   batch continues (Principle VII).
3. **Retries are visible**: re-running a failed doc appends new audit rows with incremented
   `retry_count`; prior rows are retained (Principle X).
4. **Gate decision is auditable**: `QUALITY_GATE` row records PASS or STOP with the counts summary in
   `error_message`/notes so the stop/go decision is traceable (PLAN.md §17).
