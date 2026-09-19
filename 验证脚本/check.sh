#!/usr/bin/env bash
set -euo pipefail
CHECK_HOME="$(cd "$(dirname "$0")" && pwd)"
[ "$#" -eq 1 ] || { echo '用法：check.sh <要验证的项目目录>' >&2; exit 2; }
CHECK_TARGET="$(cd "$1" && pwd)"
[ -d "$CHECK_TARGET/node_modules" ] || { echo '请先在目标项目运行 ./run.sh install' >&2; exit 2; }
CHECK_WORK="$(mktemp -d "${TMPDIR:-/tmp}/erp-cache-check.XXXXXXXX")"
cleanup() { if [ -f "$CHECK_WORK/run.sh" ]; then (cd "$CHECK_WORK" && ./run.sh down) >/dev/null 2>&1 || true; fi; rm -rf "$CHECK_WORK"; }
trap cleanup EXIT
rsync -a --exclude=.git --exclude=node_modules --exclude=dist --exclude=coverage "$CHECK_TARGET/" "$CHECK_WORK/"
ln -s "$CHECK_TARGET/node_modules" "$CHECK_WORK/node_modules"
cp "$CHECK_HOME/refresh.spec.ts" "$CHECK_HOME/harness.ts" "$CHECK_HOME/worker.ts" "$CHECK_HOME/clock.cjs" "$CHECK_WORK/tests/"
cd "$CHECK_WORK"
./run.sh test
