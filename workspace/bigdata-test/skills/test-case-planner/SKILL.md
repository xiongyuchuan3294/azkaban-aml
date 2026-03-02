---
name: test-case-planner
description: Generate AML test cases and assertions from requirement items and test scope. Use when requirement_items and test_scope are available and executable positive/boundary/negative cases are needed.
---

# Test Case Planner

## Overview

Generate structured test cases from requirement logic and scope.
Include positive, boundary, and negative scenarios with assertions.

## Inputs

- Upstream artifact: `requirement_items.json`.
- Upstream artifact: `test_scope.yaml`.
- Optional case templates and scenario tags.
- Metadata fields: `requirement_id`, `run_id`, `status`, `error_code`.

## Outputs

- Primary artifact: `test_cases.yaml`.
- Case records include case id, scenario type, preconditions, and assertions.
- Metadata fields: `requirement_id`, `run_id`, `status`, `error_code`.

## Core Workflow

1. Bind requirement items to scope entities.
2. Generate positive scenario cases.
3. Generate boundary and negative scenario cases.
4. Attach assertion definitions and expected outcomes.
5. Emit `test_cases.yaml`.

## Failure Handling

- Return `status=failed` when upstream artifacts are missing.
- Return `status=blocked` when case generation yields no executable case.
- Populate `error_code` with generation failure category.

## Validation Checklist

- Ensure each requirement item has at least one linked case.
- Ensure each case has at least one assertion.
- Ensure scenario types are explicit and valid.

## References To Load

- `references/api_reference.md`: placeholder for case schema and assertion conventions.

## Extension Notes

- Add reusable case libraries for AML high-risk scenarios.
- Add deterministic case id rules for traceability.
