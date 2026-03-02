---
name: test-scope-planner
description: Plan AML test scope from impact DAG into node-level, chain-level, and domain-level scope sets. Use when impact_dag is ready and scope boundaries must be defined before case generation.
---

# Test Scope Planner

## Overview

Convert impact DAG into explicit and prioritized test scopes.
Separate daily targeted scope from wider regression scope.

## Inputs

- Upstream artifact: `impact_dag.json`.
- Optional policy for scope mode (daily or regression).
- Metadata fields: `requirement_id`, `run_id`, `status`, `error_code`.

## Outputs

- Primary artifact: `test_scope.yaml`.
- Scope categories: node-level, chain-level, domain-level.
- Metadata fields: `requirement_id`, `run_id`, `status`, `error_code`.

## Core Workflow

1. Load impact DAG and classify impacted entities.
2. Build node-level scope list.
3. Build chain-level and domain-level scope candidates.
4. Prioritize by risk and execution budget.
5. Emit `test_scope.yaml`.

## Failure Handling

- Return `status=failed` when impact DAG is invalid.
- Return `status=blocked` when no executable scope exists.
- Populate `error_code` with scope planning category.

## Validation Checklist

- Ensure all scopes map back to impacted DAG entities.
- Ensure scope mode policy is explicit.
- Ensure high-risk paths are included.

## References To Load

- `references/api_reference.md`: placeholder for scope schema and prioritization rules.

## Extension Notes

- Add configurable budget-based scope reduction.
- Add environment-specific scope profiles.
