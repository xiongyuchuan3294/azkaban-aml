---
name: report-archiver
description: Build AML test traceability matrix and archive run evidence into requirement-level reports. Use when upstream analysis, execution, and validation artifacts are ready and auditable outputs are required.
---

# Report Archiver

## Overview

Aggregate upstream artifacts into auditable test reports.
Persist evidence with requirement-level traceability.

## Inputs

- Upstream artifacts: `requirement_items.json`, `diff_inventory.json`, `requirement_diff_mapping.json`, `impact_dag.json`, `test_scope.yaml`, `test_cases.yaml`, `data_plan.yaml`, `execution_log.json`, `assertion_result.json`.
- Metadata fields: `requirement_id`, `run_id`, `status`, `error_code`.

## Outputs

- Primary artifact: `traceability_matrix.json` or `traceability_matrix.csv`.
- Requirement-level test report and archived evidence bundle.
- Metadata fields: `requirement_id`, `run_id`, `status`, `error_code`.

## Core Workflow

1. Collect and verify upstream artifacts completeness.
2. Generate requirement-to-change-to-test traceability matrix.
3. Build report summary and attach detailed evidence references.
4. Organize archive by requirement id and run id.
5. Emit traceability matrix and report artifacts.

## Failure Handling

- Return `status=failed` when mandatory artifacts are missing.
- Return `status=blocked` when traceability cannot be completed.
- Populate `error_code` with archive and trace category.

## Validation Checklist

- Ensure traceability matrix links requirement, change, case, and assertion.
- Ensure archived evidence paths are resolvable.
- Ensure report summary matches assertion outputs.

## References To Load

- `references/api_reference.md`: placeholder for report schema and archive conventions.

## Extension Notes

- Add compliance-specific report layouts for audit scenarios.
- Add historical comparison sections for regression trend analysis.
