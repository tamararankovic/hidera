#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 <i> <value>"
  exit 1
fi

i="$1"
value="$2"

PORT=$((9200 + i))
URL="http://localhost:${PORT}/metrics"

data="# HELP app_memory_usage_bytes Current memory usage in bytes
# TYPE app_memory_usage_bytes gauge
app_memory_usage_bytes $value"

curl -X POST "$URL" \
     -H "Content-Type: text/plain; version=0.0.4" \
     --data-binary "$data"
