# source scripts/replay_s3.sh ; replay_day 2017-11-21
latest() {
  aws glue get-job-runs --job-name cartflow-clean-daily --max-items 1 \
    --query 'JobRuns[0].[Id,JobRunState]' --output text
}

replay_day() {
  local d=$1 prev_id _ id state
  read -r prev_id _ <<<"$(latest)"
  python generator/replay.py --day "$d" --target s3 --bucket "$RAW_BUCKET" || return 1
  while :; do
    sleep 10
    read -r id state <<<"$(latest)"
    case $state in SUCCEEDED|FAILED|ERROR|TIMEOUT|STOPPED) [ "$id" != "$prev_id" ] && break;; esac
  done
  echo "$d -> $state"
  [ "$state" = SUCCEEDED ]
}