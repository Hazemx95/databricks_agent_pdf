# Contract: Fixed Schema v2 + `ai_query` Extraction

This is the AI extraction contract. The same schema is returned for **every** PDF (PLAN.md §12–§13).
`schema_version = "v2"`.

## `from_json` target type (parse the model's JSON string into this)

```text
STRUCT<
  document_title:               STRING,
  document_type:                STRING,
  part_number:                  STRING,
  manufacturer:                 STRING,
  description:                  STRING,
  product_family:               STRING,
  product_category:             STRING,
  part_number_candidates:       ARRAY<STRING>,
  manufacturer_candidates:      ARRAY<STRING>,
  electrical_specifications:    MAP<STRING,STRING>,
  mechanical_specifications:    MAP<STRING,STRING>,
  environmental_specifications: MAP<STRING,STRING>,
  compliance_specifications:    MAP<STRING,STRING>,
  general_specifications:       MAP<STRING,STRING>,
  extraction_confidence:        DOUBLE,
  extraction_notes:             STRING
>
```

## `ai_query` call shape (reference, not final code)

```sql
ai_query(
  '<workspace-model-endpoint>',          -- recorded in audit.model_endpoint
  CONCAT(:extraction_instructions, '\n\nDOCUMENT:\n', parsed_content),
  responseFormat => '{"type":"json_object"}',
  failOnError    => false                 -- returns STRUCT<response, error>; no batch abort
) AS ai_response
-- raw_extraction_json := ai_response.response   (always preserved)
-- extraction_error    := ai_response.error
```

## Extraction instructions (verbatim intent from PLAN.md §13)

```text
Extract product/datasheet information from this PDF.
Use only values supported by the document content.
Do not invent values.
If a field is missing, return null.
If multiple possible part numbers exist, fill part_number_candidates.
If one part number is clearly the main one, fill part_number.
Group specifications into electrical, mechanical, environmental, compliance, and general maps.
Keep units inside values.
Return the same schema every time.
```

## Rules (enforced in extraction + validation phases)

| Rule | Enforcement |
|------|-------------|
| Same schema for every PDF | `from_json` to the fixed STRUCT above; absent keys → NULL |
| Missing scalar → NULL | normalization step |
| Missing array → NULL or `[]` | normalization step |
| Missing map → NULL or `{}` | normalization step |
| No invented values | prompt instruction + `extraction_notes` review at quality gate |
| URL/filename = hints only | `url_part_hint` never copied into `part_number` unless content confirms |
| Preserve raw output | `raw_extraction_json := ai_response.response`, kept even on `VALIDATION_FAILED` |
| Units inside values | prompt instruction |

## Validation → `extraction_status` (PLAN.md §15)

```text
SUCCESS            : valid JSON + all fixed root fields present + core fields extracted
PARTIAL_SUCCESS    : valid schema but >=1 core business field (part_number/manufacturer/description) NULL
VALIDATION_FAILED  : invalid structure or incompatible types  (raw_extraction_json still preserved)
FAILED             : ai_query/parse failed (ai_response.error set, or upstream parse_status=PARSE_FAILED)
```

Validation checks (all must hold for non-FAILED): output is valid JSON; all fixed root fields exist;
empty strings converted to NULL; arrays are arrays; maps are valid key→value; `extraction_confidence`
is numeric if present.
