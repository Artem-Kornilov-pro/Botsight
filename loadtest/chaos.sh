#!/usr/bin/env bash
# Chaos tests of Botsight.md, 18.3 against the docker compose stack.
#
# Each scenario runs loadgen like a fleet of SDKs (the same batch_id on every retry, for
# up to 3 minutes), breaks one part of the stack under that load, repairs it and then
# reconciles ClickHouse with the loadgen journal of accepted events:
#
#   rows in ClickHouse = unique event_ids = journal lines, and no batch given up.
#
# Usage, from the repository root, with the stack up (see loadtest/README.md):
#   loadtest/chaos.sh                     # every scenario
#   loadtest/chaos.sh kafka_stop network  # some of them
set -euo pipefail
cd "$(dirname "$0")/.."

COMPOSE=(docker compose -f deploy/docker-compose.yml --profile chaos)
CH="http://localhost:8123/?user=botsight&password=botsight&database=botsight"
OUT=${OUT:-/tmp/botsight-chaos}
RATE=${RATE:-3000}         # events per second in total
DURATION=${DURATION:-45s}  # load per scenario; faults start ~10 s in
REPORT="$OUT/report.md"
mkdir -p "$OUT"

log() { printf '\n== %s %s\n' "$(date +%T)" "$*"; }

(cd backend && go build -o "$OUT/loadgen" ./cmd/loadgen)

# start_load NAME COLLECTOR_URL: loadgen in the background, its pid in LOAD_PID
start_load() {
  "$OUT/loadgen" -collector "$2" -projects 2 -users 300 -rate "$RATE" -duration "$DURATION" \
    -concurrency 8 -batch 100 -duplicates 0.02 -retry-for 3m -journal "$OUT/$1.journal" ${EXTRA:-} > "$OUT/$1.txt" 2>&1 &
  LOAD_PID=$!
}

wait_load() {
  wait "$LOAD_PID" || { cat "$OUT/$1.txt"; return 1; }
}

ch() { curl -fsS "$CH" --data-binary "$1"; }

# reconcile NAME FAULT: compare ClickHouse with the journal once ch-writer caught up
reconcile() {
  local name=$1 fault=$2 txt="$OUT/$1.txt"
  local ids expected rows unique gave_up lost_answers statuses seconds
  ids=$(grep -o 'IN ([0-9,]*)' "$txt")
  expected=$(($(sort -u "$OUT/$name.journal" | wc -l)))
  # the journal itself must not repeat an event: that would be a bug of the generator,
  # not of Botsight, and would hide behind the comparison
  local journal_dups=$(($(wc -l < "$OUT/$name.journal") - expected))
  gave_up=$(grep -o 'batches given up [0-9]*' "$txt" | grep -o '[0-9]*$')
  lost_answers=$(grep -o 'lost answer [0-9]*' "$txt" | grep -o '[0-9]*$')
  statuses=$(grep -o 'statuses map\[[^]]*\]' "$txt" | sed 's/statuses map\[//; s/\]//')
  for seconds in $(seq 0 3 180); do
    read -r rows unique < <(ch "SELECT count(), uniqExact(event_id) FROM events WHERE project_id $ids FORMAT TSV")
    [ "$rows" -ge "$expected" ] && break
    sleep 3
  done
  local verdict=PASS
  if [ "$rows" != "$expected" ] || [ "$unique" != "$expected" ] || [ "$gave_up" != "0" ] || [ "$journal_dups" != "0" ]; then
    verdict=FAIL
  fi
  printf '| %s | %s | %s | %s | %s | %s | %s | %s | %s |\n' \
    "$name" "$fault" "$expected" "$rows" "$((rows - unique))" "$gave_up" "$lost_answers" "${statuses:-}" "$verdict" >> "$REPORT"
  log "$name: journal $expected, rows $rows, unique $unique, given up $gave_up, lost answers $lost_answers, caught up in ${seconds}s: $verdict"
  [ "$verdict" = PASS ]
}

wait_ready() {
  for _ in $(seq 1 90); do
    curl -fsS "localhost:$1/readyz" > /dev/null 2>&1 && return 0
    sleep 1
  done
  echo "port $1 not ready" >&2
  return 1
}

# --- scenarios ---

collector_kill() {
  start_load collector_kill http://localhost:8081
  sleep 12
  log "kill -9 collector"; "${COMPOSE[@]}" kill -s SIGKILL collector
  sleep 5
  log "start collector"; "${COMPOSE[@]}" start collector; wait_ready 8081
  wait_load collector_kill
  reconcile collector_kill "kill -9 collector на 5 с"
}

chwriter_kill() {
  start_load chwriter_kill http://localhost:8081
  sleep 12
  log "kill -9 ch-writer"; "${COMPOSE[@]}" kill -s SIGKILL chwriter
  sleep 8
  log "start ch-writer"; "${COMPOSE[@]}" start chwriter; wait_ready 8082
  wait_load chwriter_kill
  reconcile chwriter_kill "kill -9 ch-writer посреди пачек"
}

clickhouse_pause() {
  start_load clickhouse_pause http://localhost:8081
  sleep 10
  log "pause ClickHouse"; "${COMPOSE[@]}" pause clickhouse
  sleep 30
  local lag
  lag=$(curl -fsS localhost:8082/metrics | awk '/^botsight_consumer_lag\{.*events.raw/ {s += $2} END {print s + 0}')
  log "lag after 30 s: $lag"; echo "$lag" > "$OUT/clickhouse_pause.lag"
  "${COMPOSE[@]}" unpause clickhouse
  wait_load clickhouse_pause
  reconcile clickhouse_pause "ClickHouse на паузе 30 с (лаг $lag)"
}

kafka_stop() {
  start_load kafka_stop http://localhost:8081
  sleep 10
  log "stop Kafka"; "${COMPOSE[@]}" stop kafka
  sleep 20
  log "start Kafka"; "${COMPOSE[@]}" start kafka
  wait_load kafka_stop
  reconcile kafka_stop "Kafka остановлена на 20 с"
}

network() {
  local api=http://localhost:8474
  curl -fsS -X DELETE "$api/proxies/collector" > /dev/null 2>&1 || true
  curl -fsS "$api/proxies" -d '{"name":"collector","listen":"0.0.0.0:18081","upstream":"collector:8081"}' > /dev/null
  # +300 ms on the answers, and 10 % of the answers reset. Toxicity in toxiproxy picks
  # connections, so the loadgen opens one per request (EXTRA=-keepalive=false). A reset
  # answer is a batch the collector has already accepted: the loadgen retries, gets
  # "duplicate", and must count those events exactly once.
  curl -fsS "$api/proxies/collector/toxics" -d '{"name":"latency","type":"latency","stream":"downstream","attributes":{"latency":300,"jitter":100}}' > /dev/null
  curl -fsS "$api/proxies/collector/toxics" -d '{"name":"reset","type":"reset_peer","stream":"downstream","toxicity":0.1,"attributes":{"timeout":0}}' > /dev/null
  EXTRA=-keepalive=false start_load network http://localhost:18081
  wait_load network
  curl -fsS -X DELETE "$api/proxies/collector" > /dev/null
  reconcile network "toxiproxy: +300 мс, 10 % ответов оборваны"
}

all=(collector_kill chwriter_kill clickhouse_pause kafka_stop network)
selected=("$@")
[ ${#selected[@]} -eq 0 ] && selected=("${all[@]}")

{
  echo "| Сценарий | Сбой | В журнале | Строк в ClickHouse | Дублей | Брошено батчей | Ответ потерян, батч принят | Ответы collector | Итог |"
  echo "|---|---|---|---|---|---|---|---|---|"
} > "$REPORT"

failed=0
for s in "${selected[@]}"; do
  log "scenario $s"
  "$s" || failed=1
done
echo
cat "$REPORT"
exit $failed
