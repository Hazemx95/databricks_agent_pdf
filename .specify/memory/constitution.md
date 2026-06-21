<!--
SYNC IMPACT REPORT
==================
Version change: TEMPLATE (unfilled) → 1.0.0
Rationale: Initial ratification. The constitution file previously contained only
unfilled placeholder tokens; this is the first concrete adoption (MAJOR baseline 1.0.0).

Principles defined (12, per project input):
  I.    Databricks-First Execution
  II.   MCP Workspace Access
  III.  Secret Safety
  IV.   Free-Tier Cost & Quota Awareness
  V.    Unity Catalog Source of Truth
  VI.   Canonical Input File (pdf_input.txt)
  VII.  Fixed-Schema Extraction
  VIII. No Invented Values
  IX.   Audit-First Design
  X.    Retry-Safe Design
  XI.   Isolated Phase Implementation
  XII.  POC Completion Rule

Added sections:
  - Environment & Unity Catalog Standards
  - Development Workflow & Quality Gates
  - Governance

Removed sections: none (template placeholders replaced).

Templates requiring updates:
  ✅ .specify/templates/plan-template.md — Constitution Check is generic ("[Gates
     determined based on constitution file]"); no hardcoded principle drift. No edit needed.
  ✅ .specify/templates/spec-template.md — no constitution/principle references. No edit needed.
  ✅ .specify/templates/tasks-template.md — no constitution/principle references. No edit needed.
  ✅ .specify/templates/checklist-template.md — generic; no edit needed.

Follow-up TODOs: none. RATIFICATION_DATE set to first adoption date (2026-06-21).
-->

# Databricks PDF Scraping & Fixed-Schema AI Extraction POC Constitution

## Core Principles

### I. Databricks-First Execution

All real pipeline logic MUST be executed and validated inside the Databricks workspace.
An implementation that only works on the local machine is NOT accepted. The local machine
MAY be used only for Git operations, editing, Spec Kit, MCP connection, and orchestration —
never for running production pipeline steps that the POC depends on for acceptance.

**Rationale**: The deliverable is a Databricks pipeline; proving behavior anywhere else does
not demonstrate the POC works in its target environment.

### II. MCP Workspace Access

The coding agent MUST connect to the Databricks workspace through the MCP server. Workspace
configuration MUST be read from the local `.databrickscfg` profile. Connectivity and the
target profile MUST be verified before any workspace-mutating step runs.

**Rationale**: A single, declared access path keeps execution reproducible and auditable, and
ties every action to a known workspace and identity.

### III. Secret Safety (NON-NEGOTIABLE)

`.databrickscfg`, workspace tokens, PAT tokens, passwords, and any other secrets MUST NEVER be
committed. Only placeholder files such as `.databrickscfg.example` (with placeholder values
only) MAY be committed. Secrets MUST be confirmed ignored by Git before any commit.

**Rationale**: Leaked credentials are unrecoverable once pushed; prevention is the only
acceptable control.

### IV. Free-Tier Cost & Quota Awareness

This POC runs on free Databricks and MUST be cost-aware and quota-aware. Processing MUST start
with a small batch (1–5 PDFs) and expand only after first-batch quality is validated. Steps
MUST avoid unnecessary recompute and MUST be sized to stay within free-tier limits.

**Rationale**: Free-tier quotas are finite; uncontrolled full-batch runs can exhaust quota
before correctness is proven.

### V. Unity Catalog Source of Truth

All durable inputs and outputs MUST use Unity Catalog objects (Volumes and Delta tables under
the declared catalog/schema). Durable state MUST NOT live in local files, notebook scope, or
ad-hoc locations outside Unity Catalog.

**Rationale**: Centralizing state in Unity Catalog makes inputs/outputs governed, discoverable,
and retry-safe across runs.

### VI. Canonical Input File (pdf_input.txt)

The single source of truth for PDF URLs is
`/Volumes/databricks_arrow_cata/main/pdf_ai/input_urls/pdf_input.txt`. The implementation MUST
read from this file. The old `pdf_urls.txt` name MUST NOT be used, and hardcoded local URLs
MUST NOT be relied upon.

**Rationale**: One canonical input prevents divergence between what is processed and what the
project intends to process.

### VII. Fixed-Schema Extraction

Every PDF MUST produce the same final output schema (fixed schema v2). Missing scalar fields
MUST become NULL; missing arrays/maps MUST become NULL or empty structures. One bad PDF MUST
NOT break the full batch — failures are isolated per `doc_id` and recorded, not propagated.

**Rationale**: A stable schema makes downstream consumption deterministic and lets partial
failures be handled row-by-row instead of aborting the run.

### VIII. No Invented Values

AI extraction MUST NOT invent part numbers, manufacturers, descriptions, specifications, or
product details. Only values supported by the PDF content may populate extracted fields.
URL- and filename-derived values (e.g., `url_part_hint`) MAY be stored only as metadata or
hints and MUST NOT be promoted to confirmed extracted fields unless the PDF content confirms
them.

**Rationale**: Fabricated data is worse than missing data; the POC's value depends on
extracted fields being trustworthy and traceable to source content.

### IX. Audit-First Design

Every major step MUST write status and error information to the audit table. Audited steps MUST
include at minimum: URL read, URL cleaning, PDF download/staging, PDF binary read, PDF parsing,
AI extraction, JSON validation, and Delta write. Each audit record MUST capture step name,
status, error message (when applicable), and timing.

**Rationale**: Without per-step audit trails, failures in a multi-stage batch cannot be
diagnosed or retried with confidence.

### X. Retry-Safe Design

Failed PDFs MUST be retryable without duplicating successful results. Retries MUST key on
`doc_id`, increment a retry count, keep the latest error, and write final results via an
idempotent operation (e.g., MERGE or delete-then-insert by `doc_id`).

**Rationale**: Free-tier and network constraints make partial failures normal; safe retries
let the batch converge without corrupting good rows.

### XI. Isolated Phase Implementation

Each phase MUST be implemented and tested independently inside Databricks before the next phase
begins. A phase is complete only when its outputs are verified in the workspace against the
inputs the next phase will consume.

**Rationale**: Validating phase-by-phase localizes defects and prevents compounding errors
across the pipeline.

### XII. POC Completion Rule

The POC is complete ONLY when a small batch runs end-to-end inside Databricks:
`pdf_input.txt → PDF files → parsed documents → AI fixed schema → Delta result table →
audit table`. Partial flows do not satisfy completion.

**Rationale**: End-to-end execution on real inputs is the only evidence that the integrated
pipeline works as intended.

## Environment & Unity Catalog Standards

The POC operates against the following declared Unity Catalog environment. These values are
binding unless amended via the Governance process:

- **Catalog**: `databricks_arrow_cata`
- **Schema**: `main`
- **Volume**: `pdf_ai`
- **Input URL file**: `/Volumes/databricks_arrow_cata/main/pdf_ai/input_urls/pdf_input.txt`
- **Raw PDF folder**: `/Volumes/databricks_arrow_cata/main/pdf_ai/raw_pdfs/`

Standards:

- Durable tables MUST be created under `databricks_arrow_cata.main`.
- Both automatic URL download and manual/staged PDF modes MUST converge on the same
  `raw_pdfs` Volume and the same downstream `read_files(binaryFile)` path.
- URL handling MUST support plain `http(s)` URLs and HTML anchor-tag URLs, deduplicate cleaned
  URLs, and preserve the original `source_url` for traceability.

## Development Workflow & Quality Gates

- Work proceeds in the phases defined by `PLAN.md`; each phase is gated by Principle XI
  (isolated, workspace-verified completion).
- Before expanding beyond the initial 1–5 PDF batch, a quality gate MUST evaluate extraction
  outcomes (SUCCESS / PARTIAL_SUCCESS / FAILED counts and core-field NULL rates). If most core
  fields are NULL, full processing MUST stop until the schema and extraction instructions are
  improved (Principle IV + VII).
- Every change that touches credentials, committed files, or Unity Catalog objects MUST be
  checked against Principles III and V before merge.
- Plans, specs, and tasks generated by Spec Kit MUST be checkable against these principles; any
  deviation MUST be recorded in the plan's Complexity Tracking with justification.

## Governance

This constitution supersedes other practices for this POC. When guidance conflicts, the
constitution wins.

- **Amendments**: Any change to principles, the declared environment, or governance MUST be
  made by editing this file, accompanied by an updated Sync Impact Report and a version bump.
- **Versioning policy** (semantic):
  - **MAJOR**: Backward-incompatible governance/principle removals or redefinitions.
  - **MINOR**: A new principle/section is added or guidance is materially expanded.
  - **PATCH**: Clarifications, wording, or non-semantic refinements.
- **Compliance review**: Plans and PRs MUST verify compliance with these principles. Any
  violation MUST be justified in the plan's Complexity Tracking or remediated before merge.
- **Runtime guidance**: Use `PLAN.md` for detailed phase-level execution guidance; it MUST stay
  consistent with this constitution, and on conflict this constitution prevails.

**Version**: 1.0.0 | **Ratified**: 2026-06-21 | **Last Amended**: 2026-06-21
