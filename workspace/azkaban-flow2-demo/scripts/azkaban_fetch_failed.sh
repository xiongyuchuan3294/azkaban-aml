#!/usr/bin/env bash
set -euo pipefail

export LC_ALL=C

AZKABAN_URL="${AZKABAN_URL:-http://127.0.0.1:8081}"
AZKABAN_USER="${AZKABAN_USER:-azkaban}"
AZKABAN_PASS="${AZKABAN_PASS:-azkaban}"
PROJECT="${1:-AML}"
FLOW="${2:-basic}"
EXEC_HINT="${3:-}"
FLOW_PAGE_SIZE="${FLOW_PAGE_SIZE:-30}"
JOB_LOG_LEN="${JOB_LOG_LEN:-60000}"

json_get() {
  local endpoint="$1"
  curl -sS "$endpoint"
}

login_json=$(curl -sS -X POST "${AZKABAN_URL}/" \
  --data "action=login&username=${AZKABAN_USER}&password=${AZKABAN_PASS}")

session_id=$(printf '%s' "$login_json" | perl -ne 'if(/"session.id"\s*:\s*"([^"]+)"/){print $1; exit}')
if [[ -z "${session_id:-}" ]]; then
  echo "Login failed. Check AZKABAN_URL/AZKABAN_USER/AZKABAN_PASS." >&2
  exit 1
fi

if [[ -n "$EXEC_HINT" ]]; then
  exec_id="$EXEC_HINT"
else
  flows_json=$(json_get "${AZKABAN_URL}/manager?ajax=fetchFlowExecutions&project=${PROJECT}&flow=${FLOW}&start=0&length=${FLOW_PAGE_SIZE}&session.id=${session_id}")
  exec_candidates=$(printf '%s' "$flows_json" | perl -ne 'while(/"execId"\s*:\s*(\d+)/g){print "$1\n"}')
  exec_id=""
  exec_json=""
  failed_jobs=""

  while IFS= read -r candidate; do
    [[ -z "$candidate" ]] && continue
    candidate_json=$(json_get "${AZKABAN_URL}/executor?ajax=fetchexecflow&execid=${candidate}&session.id=${session_id}")
    candidate_failed=$(printf '%s' "$candidate_json" \
      | perl -0777 -ne 'while(/\{[^{}]*"id"\s*:\s*"([^"]+)"[^{}]*"status"\s*:\s*"FAILED"[^{}]*\}/sg){ print "$1\n"; }' \
      | awk '!seen[$0]++')
    if [[ -n "${candidate_failed:-}" ]]; then
      exec_id="$candidate"
      exec_json="$candidate_json"
      failed_jobs="$candidate_failed"
      break
    fi
  done <<< "$exec_candidates"

  if [[ -z "${exec_id:-}" ]]; then
    exec_id=$(printf '%s' "$exec_candidates" | sed -n '1p')
  fi
fi

if [[ -z "${exec_id:-}" ]]; then
  echo "No execution found for project=${PROJECT}, flow=${FLOW}" >&2
  exit 1
fi

if [[ -z "${exec_json:-}" ]]; then
  exec_json=$(json_get "${AZKABAN_URL}/executor?ajax=fetchexecflow&execid=${exec_id}&session.id=${session_id}")
fi
flow_status=$(printf '%s' "$exec_json" | perl -ne 'if(/"status"\s*:\s*"([^"]+)"/){print $1; exit}')

echo "project=${PROJECT} flow=${FLOW} exec_id=${exec_id} status=${flow_status}"

echo "failed_jobs:"
if [[ -z "${failed_jobs:-}" ]]; then
  failed_jobs=$(printf '%s' "$exec_json" \
    | perl -0777 -ne 'while(/\{[^{}]*"id"\s*:\s*"([^"]+)"[^{}]*"status"\s*:\s*"FAILED"[^{}]*\}/sg){ print "$1\n"; }' \
    | awk '!seen[$0]++')
fi

if [[ -z "${failed_jobs:-}" ]]; then
  echo "  (none)"
  exit 0
fi

while IFS= read -r job_id; do
  [[ -z "$job_id" ]] && continue
  [[ "$job_id" == "$FLOW" ]] && continue
  echo "  - ${job_id}"
done <<< "$failed_jobs"

echo
while IFS= read -r job_id; do
  [[ -z "$job_id" ]] && continue
  [[ "$job_id" == "$FLOW" ]] && continue

  log_json=$(json_get "${AZKABAN_URL}/executor?ajax=fetchExecJobLogs&execid=${exec_id}&jobId=${job_id}&offset=0&length=${JOB_LOG_LEN}&session.id=${session_id}")
  log_text=$(printf '%s' "$log_json" | perl -0777 -ne '
    if(/"data"\s*:\s*"(.*)"\s*}/s){
      $d=$1;
      $d =~ s/\\n/\n/g;
      $d =~ s/\\r/\r/g;
      $d =~ s/\\t/\t/g;
      $d =~ s/\\"/"/g;
      $d =~ s/\\\\/\\/g;
      print $d;
    }
  ')

  echo "===== ${job_id} (exec ${exec_id}) ====="
  if [[ -n "$log_text" ]]; then
    printf '%s\n' "$log_text"
  else
    echo "(empty log)"
  fi
  echo
  echo "----- key error lines: ${job_id} -----"
  printf '%s\n' "$log_text" | rg -n "ERROR|Exception|FAILED|Error:|ProcessFailureException|return code|MetaException" -S || true
  echo

done <<< "$failed_jobs"
