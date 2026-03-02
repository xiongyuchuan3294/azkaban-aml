---
name: result-validator
description: Validate AML test outputs with integrity, accuracy, and timeliness assertions. Use when execution logs and assertion definitions are available and automated verdicts are required.
---

# Result Validator

## Overview

Apply assertion rules to execution outputs.
Generate machine-readable verdicts and diff samples.

## Inputs

- Upstream artifact: `execution_log.json`.
- Assertion definitions from `test_cases.yaml` or rule sources.
- Optional baseline references for comparison checks.
- Metadata fields: `requirement_id`, `run_id`, `status`, `error_code`.

## Outputs

- Primary artifact: `assertion_result.json`.
- Pass/fail verdicts, failed assertion details, and sample differences.
- Metadata fields: `requirement_id`, `run_id`, `status`, `error_code`.

## Core Workflow

1. Load assertions and execution outputs.
2. Run integrity checks.
3. Run accuracy and timeliness checks.
4. Build verdict summary and diff samples.
5. Emit `assertion_result.json`.

## Failure Handling

- Return `status=failed` when assertion evaluation cannot proceed.
- Return `status=blocked` when required execution evidence is missing.
- Populate `error_code` with validation category.

## Validation Checklist

- Ensure each assertion has deterministic verdict.
- Ensure failed assertions include evidence references.
- Ensure summary counts match detail records.

## References To Load

- `references/api_reference.md`: placeholder for assertion schemas and verdict enums.

## Extension Notes

- Add domain-specific AML rule packs.
- Add threshold drift alerting for long-term stability checks.
