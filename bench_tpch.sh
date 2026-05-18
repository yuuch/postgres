#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "$ROOT/contrib/pg_yaap/src/executor/bench_tpch.sh" "$@"
