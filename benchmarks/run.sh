#!/usr/bin/env bash
# Reproduces the numbers in the README.
#   benchmarks/run.sh local    in-process and TCP, no external services
#   benchmarks/run.sh docker   Redis and Kafka; needs `docker compose up -d redis redpanda`
#   benchmarks/run.sh kafka    Kafka only
# Every load run has a time limit and records the machine's load average beside its result,
# so a run taken during a machine-wide stall is visible rather than silently averaged in.
set -u
cd "$(dirname "$0")/.."
OUT=benchmarks/results
mkdir -p "$OUT"
PY=python/.venv/bin/python
ORDER=19000
FEED=19001
ROUNDS=3

load_avg() { uptime | sed 's/.*load averages*: *//' | awk '{print $1}' | tr -d ','; }

run_engine() {
  ./build/engine --port $ORDER --feed-port $FEED --loopback "$@" > "$OUT/.engine.log" 2>&1 &
  ENGINE_PID=$!
  sleep 1
}
# Call this directly, not inside $(...): wait only works on a child of the current shell.
stop_engine() { kill -TERM "$ENGINE_PID" 2>/dev/null; wait "$ENGINE_PID" 2>/dev/null; ENGINE_SUMMARY=$(tail -1 "$OUT/.engine.log"); }

# args: file label limit-seconds loadgen-args...
measure() {
  local file=$1 label=$2 limit=$3; shift 3
  local before result
  before=$(load_avg)
  result=$(timeout "$limit" ./build/loadgen --port $ORDER "$@" 2>/dev/null) || result='{"timed_out":true}'
  echo "{\"label\":\"$label\",\"load_avg\":$before,\"result\":$result}" | tee -a "$file"
}

section_local() {
  echo "== in-process book and rings"
  ./build/bench --events 5000000 | tee "$OUT/inprocess.json"

  : > "$OUT/tcp.jsonl"
  echo "== TCP capacity, flat out"
  for round in $(seq $ROUNDS); do
    run_engine
    measure "$OUT/tcp.jsonl" "capacity-1conn" 120 --orders 3000000 --conns 1 --batch 64 --window 8192
    stop_engine
  done

  echo "== TCP open-loop latency"
  for spec in "5000 50000" "50000 300000" "200000 1200000" "400000 2400000"; do
    set -- $spec
    for round in $(seq $ROUNDS); do
      run_engine
      measure "$OUT/tcp.jsonl" "open-$1" 60 --orders "$2" --rate "$1" --batch 64 --coalesce-us 200
      stop_engine
    done
  done
}

section_coalesce() {
  echo "== 50,000 msgs/sec with and without send coalescing"
  : > "$OUT/coalesce.jsonl"
  for round in $(seq $ROUNDS); do
    for micros in 0 200; do
      run_engine
      measure "$OUT/coalesce.jsonl" "coalesce-$micros" 60 --orders 300000 --rate 50000 --batch 64 --coalesce-us $micros
      stop_engine
    done
  done
}

section_docker() {
  : > "$OUT/redis.jsonl"
  echo "== Redis order-state cache at 1,000 msgs/sec: blocking write-through vs async publisher"
  for round in $(seq $ROUNDS); do
    for mode in sync async; do
      docker compose exec -T redis redis-cli flushall > /dev/null
      if [ $mode = sync ]; then run_engine --redis 127.0.0.1:6379 --sync-redis; else run_engine --redis 127.0.0.1:6379; fi
      measure "$OUT/redis.jsonl" "redis-$mode" 90 --orders 20000 --rate 1000 --batch 1
      stop_engine
      echo "   engine: $ENGINE_SUMMARY"
      echo "   keys in redis: $(docker compose exec -T redis redis-cli dbsize)"
    done
  done

  section_kafka
}

section_kafka() {
  echo "== Kafka bridge at 20,000 msgs/sec"
  docker compose exec -T redpanda rpk topic delete trades-bench > /dev/null 2>&1
  docker compose exec -T redpanda rpk topic create trades-bench > /dev/null
  run_engine
  $PY python/kafka_bridge.py --feed 127.0.0.1:$FEED --bootstrap localhost:19092 --topic trades-bench 2> "$OUT/.bridge.log" &
  local bridge=$!
  sleep 3
  : > "$OUT/kafka.jsonl"
  measure "$OUT/kafka.jsonl" "kafka-bridge" 90 --orders 200000 --rate 20000 --batch 64 --coalesce-us 200
  # The bridge is slower than the feed, and the engine only drains for a second at shutdown,
  # so wait until it has stopped falling behind before stopping the engine.
  sleep 30
  stop_engine
  local summary=$ENGINE_SUMMARY
  wait $bridge
  echo "engine: $summary"
  echo "bridge: $(tail -1 "$OUT/.bridge.log")"
  sleep 2
  local delivered
  delivered=$(docker compose exec -T redpanda rpk topic describe trades-bench -p | awk 'NR>1 {s+=$NF} END {print s}')
  echo "messages in topic: $delivered"
  { echo "{\"engine\":$summary}"; echo "{\"bridge\":$(tail -1 "$OUT/.bridge.log")}"; echo "{\"messages_in_topic\":$delivered}"; } >> "$OUT/kafka.jsonl"
}

case "${1:-local}" in
  local) section_local; section_coalesce ;;
  docker) section_docker ;;
  kafka) section_kafka ;;
  *) echo "usage: $0 [local|docker]"; exit 2 ;;
esac
rm -f "$OUT/.engine.log" "$OUT/.bridge.log"
