#!/usr/bin/env bash
# Swarm watchdog: restart the engine if its API is unreachable (stale-lock safe).
if ! curl -sf http://127.0.0.1:6970/api/health --max-time 5 > /dev/null 2>&1; then
  cd /opt/data/swarm || exit 1
  rm -f data/engine.lock
  nohup .venv/bin/python app.py >> data/server.log 2>&1 &
  echo "$(date '+%F %T') swarm restarted (was unreachable)"
else
  echo "swarm healthy"
fi
