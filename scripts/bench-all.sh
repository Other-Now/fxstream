#!/usr/bin/env bash
# Run every measurement and write JSON + SUMMARY.md to $OUT. Needs Postgres, MongoDB and RabbitMQ up;
# starts and stops the service itself (twice: normal, then idempotency off for the chaos control).
#
#   OUT=results/local JDBC=jdbc:postgresql://localhost:5433/fxstream \
#   RESTART_CMD="wsl -d Ubuntu-24.04 -- /home/chiku/rmq/crash.sh" scripts/bench-all.sh
set -euo pipefail
cd "$(dirname "$0")/.."

OUT=${OUT:-results/local}
JDBC=${JDBC:-jdbc:postgresql://localhost:5432/fxstream}
RATES=${RATES:-1000,5000,10000}
RESTART_CMD=${RESTART_CMD:?set RESTART_CMD to a command that kill -9s and restarts the broker}
CHAOS="--orders ${CHAOS_ORDERS:-15000} --rate 200 --kills ${CHAOS_KILLS:-5} --first-kill-ms 5000 --kill-every-ms ${KILL_EVERY_MS:-15000}"
JAVA="${JAVA_HOME:+$JAVA_HOME/bin/}java"
B=("$JAVA" -Xmx1g -jar bench/target/fxstream-bench.jar)
mkdir -p "$OUT"

echo "== jmh (tick path, -prof gc)"
"${B[@]}" jmh TickPathBench -prof gc -rf json -rff "$OUT/jmh.json" > "$OUT/jmh.txt"
echo "== soak (core only, GC count)"
"${B[@]}" soak --out "$OUT"

scripts/app.sh start
echo "== amqp baseline"
"${B[@]}" amqp --rates 1000,5000,15000 --out "$OUT"
echo "== tick -> client price latency"
"${B[@]}" latency --rates "$RATES" --out "$OUT"
echo "== stale provider exclusion"
"${B[@]}" stale --trials 20 --out "$OUT"
echo "== conflation (slow consumer)"
"${B[@]}" conflation --out "$OUT"
echo "== quote expiry"
"${B[@]}" expiry --jdbc "$JDBC" --out "$OUT"
echo "== chaos, idempotency on"
"${B[@]}" chaos $CHAOS --restart-cmd "$RESTART_CMD" --jdbc "$JDBC" --name chaos-safe --out "$OUT"

scripts/app.sh start --fx.idempotent=false
echo "== chaos control, idempotency off"
"${B[@]}" chaos $CHAOS --restart-cmd "$RESTART_CMD" --jdbc "$JDBC" --name chaos-unsafe --out "$OUT"
scripts/app.sh stop

cp results/app-gc.log "$OUT/" 2>/dev/null || true
python3 scripts/summary.py "$OUT" > "$OUT/SUMMARY.md" 2>/dev/null || python scripts/summary.py "$OUT" > "$OUT/SUMMARY.md"
cat "$OUT/SUMMARY.md"
