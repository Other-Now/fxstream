#!/usr/bin/env bash
# Start or stop the fxstream service in the background.
#   scripts/app.sh start [spring args...]   e.g. scripts/app.sh start --fx.idempotent=false
#   scripts/app.sh stop
# Waits for /api/stats to answer before returning from start. Logs to results/app.log.
set -euo pipefail
cd "$(dirname "$0")/.."
JAVA="${JAVA_HOME:+$JAVA_HOME/bin/}java"
JAR=app/target/fxstream-app-0.1.0.jar
PORT="${FX_HTTP_PORT:-8090}"

stop() {
  if [[ "$(uname -s)" == MINGW* || "$(uname -s)" == MSYS* ]]; then
    powershell -NoProfile -Command "Get-CimInstance Win32_Process -Filter \"name='java.exe'\" | Where-Object { \$_.CommandLine -like '*fxstream-app*' } | ForEach-Object { Stop-Process -Id \$_.ProcessId -Force }" || true
  else
    pkill -f fxstream-app || true
  fi
  for _ in $(seq 1 50); do curl -sf "localhost:$PORT/api/stats" >/dev/null || return 0; sleep 0.2; done
}

case "${1:-}" in
  start)
    shift
    stop
    mkdir -p results
    "$JAVA" -Xms512m -Xmx512m -XX:+UseG1GC -Xlog:gc:file=results/app-gc.log -jar "$JAR" "$@" > results/app.log 2>&1 &
    for _ in $(seq 1 120); do
      if curl -sf "localhost:$PORT/api/stats" >/dev/null; then echo "fxstream up ($*)"; exit 0; fi
      sleep 0.5
    done
    echo "fxstream did not start; tail of results/app.log:" >&2
    tail -50 results/app.log >&2
    exit 1
    ;;
  stop) stop ;;
  *) echo "usage: $0 start [args] | stop" >&2; exit 2 ;;
esac
