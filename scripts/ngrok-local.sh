#!/usr/bin/env sh
# Start Neo4j Bolt (TCP), Neo4j Browser (HTTP), and ClickHouse (HTTP) in one ngrok agent.
#
# Merges your saved authtoken (macOS default path below) with ../ngrok-cli.yml tunnel definitions.
# Prereq: docker compose up -d

set -e
cd "$(dirname "$0")/.." || exit 1

TOKEN_CFG="${NGROK_CONFIG_PATH:-$HOME/Library/Application Support/ngrok/ngrok.yml}"
if ! test -f "$TOKEN_CFG"; then
  TOKEN_CFG="${HOME}/.config/ngrok/ngrok.yml"
fi
if ! test -f "$TOKEN_CFG"; then
  echo "No ngrok authtoken config found. Run: ngrok config add-authtoken <token>" >&2
  echo "Or set NGROK_CONFIG_PATH to a yml that contains authtoken." >&2
  exit 1
fi

exec ngrok start --all --config "$TOKEN_CFG" --config ngrok-cli.yml
