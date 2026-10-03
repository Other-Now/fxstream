#!/usr/bin/env bash
# CI: kill -9 the RabbitMQ service container and start it again (same container, same volume, so
# durable quorum queues come back from disk).
set -euo pipefail
CID=$(docker ps -aq --filter "ancestor=rabbitmq:3.13")
docker kill --signal KILL "$CID" >/dev/null
sleep 2
docker start "$CID" >/dev/null
