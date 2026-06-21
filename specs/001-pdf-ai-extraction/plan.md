# Implementation Plan: Databricks PDF URL Looping & Fixed-Schema AI Extraction POC

**Branch**: `001-pdf-ai-extraction` | **Date**: 2026-06-21 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `specs/001-pdf-ai-extraction/spec.md`

## Summary

Build a Databricks-native POC pipeline that loops over PDF URLs in
`/Volumes/databricks_arrow_cata/main/pdf_ai/input_urls/pdf_input.txt`, cleans/deduplicates them
into a Delta control table, obtains each PDF either by automatic download or by detecting
manually-staged files in `raw_pdfs/`, reads the PDFs as binary, parses them with
`ai_parse_document`, extracts product/datasheet fields into **fixed schema v2** with `ai_query`
(structured output), validates/normalizes the result, and writes it to a Delta results table —
with a full per-step audit trail and a free-tier quality gate before any full run. All durable
state lives in Unity Catalog under `databricks_arrow_cata.main` and the `pdf_ai` Volume. All real
logic runs **inside the Databricks workspace** via the MCP connection; the local machine is used
only for editing, Git, Spec Kit, MCP, and orchestration.

## Technical Context

**Language/Version**: SQL (Databricks SQL) + PySpark / Python 3.x executed on Databricks serverless
compute. Notebooks/SQL invoked through the Databricks MCP server (`execute_sql`, `execute_code`).

**Primary Dependencies**: Databricks built-in AI functions — `ai_parse_document(content,
map('version','2.0'))` for parsing and `ai_query` (with structured/JSON output) for fixed-schema-v2
extraction; Delta Lake; Unity Catalog Volumes; `read_files(..., format => 'binaryFile')`. Local:
Databricks CLI/SDK + `.databrickscfg` profile `DEFAULT` (host `dbc-25b231f6-50…`) for MCP auth.

**Storage**: Unity Catalog — catalog `databricks_arrow_cata`, schema `main`, Volume `pdf_ai`.
Four Delta tables: `pdf_input_urls`, `pdf_parsed_documents`, `datasheet_extraction_results`,
`pdf_extraction_audit`. Volume folders: `input_urls/`, `raw_pdfs/`, optional `debug/`.

**Testing**: Phase-by-phase manual verification inside Databricks via MCP (`execute_sql` count/shape
checks, `get_table_stats_and_schema`, `get_volume_folder_details`). Each phase verified against the
inputs the next phase consumes before proceeding (constitution Principle XI). No local pytest of
pipeline logic — local runs are not accepted as proof (Principle I).

**Target Platform**: Databricks **free** workspace (serverless). Outbound internet to external PDF
hosts (e.g., `download.siliconexpert.com`) may be blocked — manual/staged PDF mode is first-class.

**Project Type**: Data/AI batch pipeline (notebooks + SQL on Databricks), not an app or service.

**Performance Goals**: Not throughput-driven. POC correctness and quota safety dominate: process
**1 PDF first, then up to 5**, then full only after the quality gate passes (Principle IV).

**Constraints**: Free-tier cost/quota awareness; never stop the batch on one bad URL/PDF; never
delete existing `raw_pdfs/` files; retry-safe by `doc_id`; no invented extracted values; secrets
(`.databrickscfg`, tokens) never committed.

**Scale/Scope**: Small POC batch (1–5 PDFs for the gate; full `pdf_input.txt` list afterward). Four
tables, eleven audited steps, two acquisition modes, one fixed extraction schema (v2).

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

Mapped against `.specify/memory/constitution.md` v1.0.0 (12 principles):

| # | Principle | Plan compliance |
|---|-----------|-----------------|
| I | Databricks-First Execution | All pipeline logic runs in-workspace via MCP; local only for edit/Git/Speckit/MCP/orchestration. ✅ |
| II | MCP Workspace Access | Execution via Databricks MCP; auth from `.databrickscfg` profile `DEFAULT`. ✅ |
| III | Secret Safety | No secrets committed; only `.databrickscfg.example` may be. `.gitignore` check is a Phase-1 task. ✅ |
| IV | Free-Tier Cost & Quota | 1 → ≤5 → full staging; quality gate before full run; idempotent writes avoid recompute. ✅ |
| V | Unity Catalog Source of Truth | All inputs/outputs are UC Volumes + Delta tables under `databricks_arrow_cata.main`. ✅ |
| VI | Canonical Input File | Reads only `input_urls/pdf_input.txt`; `pdf_urls.txt` and hardcoded URLs forbidden. ✅ |
| VII | Fixed-Schema Extraction | Every PDF → fixed schema v2; missing → NULL/empty; per-`doc_id` isolation, no batch abort. ✅ |
| VIII | No Invented Values | `ai_query` prompt forbids invention; URL/filename are hints only, not promoted unless confirmed. ✅ |
| IX | Audit-First Design | All 11 steps write to `pdf_extraction_audit` with status/error/timing. ✅ |
| X | Retry-Safe Design | Retry only failed/incomplete rows keyed by `doc_id`; increment `retry_count`; idempotent MERGE. ✅ |
| XI | Isolated Phase Implementation | Plan decomposes into independently testable phases verified in-workspace. ✅ |
| XII | POC Completion Rule | Done = small batch end-to-end: input → PDFs → parsed → fixed schema → results → audit. ✅ |

**Result**: PASS — no violations. Complexity Tracking not required.

## Project Structure

### Documentation (this feature)

```text
specs/001-pdf-ai-extraction/
├── plan.md              # This file (/speckit-plan output)
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output (tables, statuses, transitions)
├── quickstart.md        # Phase 1 output (in-workspace validation guide)
├── contracts/           # Phase 1 output
│   ├── tables.sql              # DDL for the 4 Delta tables (data contract)
│   ├── extraction_schema_v2.md # Fixed schema v2 + ai_query response_format contract
│   └── audit_contract.md       # Audited step names + status enums per step
├── checklists/
│   └── requirements.md  # Spec quality checklist (already created)
└── tasks.md             # Phase 2 output (/speckit-tasks — NOT created here)
```

### Source Code (repository root)

This POC is delivered as Databricks SQL/notebook assets executed in-workspace via MCP, mirrored in
the repo for version control. There is no local application runtime.

```text
databricks/
├── sql/
│   └── 00_create_tables.sql          # Mirrors contracts/tables.sql (idempotent CREATE)
├── notebooks/
│   ├── 01_load_urls.py               # Phase 3: read pdf_input.txt → clean/dedup → pdf_input_urls
│   ├── 02_acquire_pdfs.py            # Phase 4: download OR detect staged PDFs → raw_pdfs
│   ├── 03_read_binary.py             # Phase 5: read_files(binaryFile) join to control table
│   ├── 04_parse_documents.py         # Phase 6: ai_parse_document → pdf_parsed_documents
│   ├── 05_extract_fixed_schema.py    # Phase 7: ai_query fixed schema v2 (+ raw json)
│   ├── 06_validate_normalize.py      # Phase 8: validate/normalize → status
│   ├── 07_write_results.py           # Phase 9: MERGE into datasheet_extraction_results
│   └── 08_quality_gate.py            # Phase 10: counts + stop/go signal
└── lib/
    └── pipeline_common.py            # shared helpers: url cleaning, audit writer, naming
```

**Structure Decision**: Single Databricks pipeline organized one notebook per pipeline phase plus a
shared `lib/` for cross-cutting helpers (URL cleaning, deterministic naming, audit writer). This
directly satisfies Principle XI (isolated, independently testable phases) — each notebook reads a
Delta/Volume input produced by the prior phase and writes a Delta/Volume output the next phase
consumes, so phases can be run, re-run, and verified in isolation inside the workspace. SQL DDL is
kept separate and idempotent so tables can be (re)created without touching pipeline logic. These
paths mirror PLAN.md's 10 implementation phases; no paths, tables, or scope are introduced beyond
PLAN.md.

## Complexity Tracking

> No Constitution Check violations. Section intentionally empty.
