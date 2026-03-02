---
name: test-data-generator
description: Generate, isolate, and recycle AML test data from planned test cases. Use when test_cases are ready and deterministic daily testing data setup is required before execution.
---

# Test Data Generator

## Overview

Produce executable data plans for daily and regression testing.
Guarantee data isolation, recoverability, and rerun consistency.

## Inputs

- Upstream artifact: `test_cases.yaml`.
- Optional environment settings and data volume constraints.
- Metadata fields: `requirement_id`, `run_id`, `status`, `error_code`.

## Outputs

- Primary artifact: `data_plan.yaml`.
- Data batch identifiers, isolation policy, and cleanup strategy.
- Metadata fields: `requirement_id`, `run_id`, `status`, `error_code`.

## Core Workflow

1. Parse case-level data requirements.
2. Build base, boundary, and abnormal data plans.
3. Assign isolation keys and partition strategy.
4. Define cleanup and rollback actions.
5. Emit `data_plan.yaml` with batch metadata.

## Failure Handling

- Return `status=failed` when data requirements are inconsistent.
- Return `status=blocked` when isolation policy cannot be satisfied.
- Populate `error_code` with generation or cleanup category.

## Validation Checklist

- Ensure each test case has a data strategy.
- Ensure cleanup strategy is explicit and executable.
- Ensure rerun consistency conditions are declared.

## References To Load

- `references/api_reference.md`: placeholder for data plan schema and isolation rules.

## Extension Notes

- Add synthetic data templates for common AML entities.
- Add environment-aware data volume controls.
