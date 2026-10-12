# Run one Athena statement in the cartflow workgroup and print the result.
athena() {
  local qid state
  qid=$(aws athena start-query-execution --work-group cartflow \
        --query-execution-context Database=cartflow \
        --query-string "$1" --query QueryExecutionId --output text) || return 1
  while :; do
    state=$(aws athena get-query-execution --query-execution-id "$qid" \
            --query QueryExecution.Status.State --output text)
    case $state in SUCCEEDED|FAILED|CANCELLED) break;; esac
    sleep 1
  done
  if [ "$state" = SUCCEEDED ]; then
    aws athena get-query-results --query-execution-id "$qid" \
      --query 'ResultSet.Rows[*].Data[*].VarCharValue' --output text
  else
    echo "$state:"
    aws athena get-query-execution --query-execution-id "$qid" \
      --query QueryExecution.Status.StateChangeReason --output text
    return 1
  fi
}