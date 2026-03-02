---
name: execution-orchestrator
description: Orchestrate AML test execution across Azkaban, Hive, and MySQL using planned scope, data, and cases. Use when impact_dag, data_plan, and test_cases are ready for automated run execution.
---

# Execution Orchestrator

## Overview

Coordinate multi-system execution for daily and regression test runs.
Capture stage-level logs, retries, and failure traces.

## Inputs

- Upstream artifact: `impact_dag.json`.
- Upstream artifact: `data_plan.yaml`.
- Upstream artifact: `test_cases.yaml`.
- Metadata fields: `requirement_id`, `run_id`, `status`, `error_code`.

## Outputs

- Primary artifact: `execution_log.json`.
- Execution stage status, retry traces, and failure snapshots.
- Metadata fields: `requirement_id`, `run_id`, `status`, `error_code`.

## Core Workflow

1. Validate execution prerequisites and connectivity.
2. Run precheck and data preparation stages.
3. Execute DAG tasks by dependency and strategy.
4. Apply retry and stop policies on failures.
5. Emit `execution_log.json` with stage history.

## Failure Handling

- Return `status=failed` when orchestration cannot start.
- Return `status=blocked` when critical prerequisite checks fail.
- Populate `error_code` with stage and subsystem category.

## Validation Checklist

- Ensure every stage has start/end status records.
- Ensure retry decisions are traceable.
- Ensure failed node snapshots are preserved.

## References To Load

- `references/api_reference.md`: placeholder for execution stage contracts and retry policy.

## Extension Notes

- Add mode-specific execution profiles for daily and regression runs.
- Add concurrency control policies by resource class.
