#!/usr/bin/env bash
set -euo pipefail

REGION="${REGION:-us-east-1}"
STACK_COMPUTE="${STACK_COMPUTE:-MskDemo-Compute}"
TOPIC="${TOPIC:-msk-demo-source}"

DURATION="${DURATION:-60}" # total seconds to run (120 == 2 minutes)
WORKERS="${WORKERS:-5}" # invocations per second (should equal to the number of partitions)
MESSAGES_PER_LAMBDA="${MESSAGES_PER_LAMBDA:-100}" # msg/s per lambda (WORKERS*100 = total TPS)
DELETE_PCT="${DELETE_PCT:-0}" # 0..1 for soft delete percent of messages

now_ms() {
  local out
  out="$(date +%s%3N 2>/dev/null || true)"
  if [[ "$out" =~ ^[0-9]+$ ]]; then
    echo "$out"; return
  fi
  if command -v gdate >/dev/null 2>&1; then
    out="$(gdate +%s%3N 2>/dev/null || true)"
    if [[ "$out" =~ ^[0-9]+$ ]]; then
      echo "$out"; return
    fi
  fi
  echo $(( $(date +%s) * 1000 ))
}

echo "Get LoadGeneratorFunction name from stack: $STACK_COMPUTE"
FN="$(aws cloudformation describe-stacks \
  --stack-name "$STACK_COMPUTE" \
  --region "$REGION" \
  --query "Stacks[0].Outputs[?OutputKey=='LoadGeneratorFunction'].OutputValue" \
  --output text)"

if [[ -z "$FN" || "$FN" == "None" ]]; then
  echo "ERROR: Could not get LoadGeneratorFunction from $STACK_COMPUTE output." >&2
  exit 1
fi

TOTAL_TPS=$(( WORKERS * MESSAGES_PER_LAMBDA ))
echo "Plan: ${WORKERS} x invocations/sec x ${MESSAGES_PER_LAMBDA} msg/s each = ${TOTAL_TPS} msg/s total for ${DURATION}s"
echo "Using LoadGeneratorFunction: $FN"

start_ms="$(now_ms)"
for ((t=1; t<=DURATION; t++)); do
  echo "Executing ${t} round:"
  for ((i=1; i<= WORKERS; i++)); do
    payload=$(printf '{"topic":"%s","rate":%s,"seconds":1,"deletePct":%s,"shard":%s,"shards":%s}' \
      "$TOPIC" "$MESSAGES_PER_LAMBDA" "$DELETE_PCT" "$i" "$WORKERS")

    echo "  Running worker number ${i}"
    aws lambda invoke \
      --region "$REGION" \
      --invocation-type Event \
      --function-name "$FN" \
      --cli-binary-format raw-in-base64-out \
      --payload "$payload" \
      /dev/null >/dev/null 2>&1 & # fire-and-forget this worker
  done

  # Wait for all workers for this invocation to be dispatched
  wait

  target_ms=$(( start_ms + (t+1)*1000 ))
  now="$(now_ms)"
  sleep_ms=$(( target_ms - now ))
  if (( sleep_ms > 0 )); then
    sleep "$(awk "BEGIN { printf \"%.3f\", $sleep_ms/1000 }")"
  fi
done

echo "Finished running for ${DURATION} seconds at ${TOTAL_TPS} msg/s."
