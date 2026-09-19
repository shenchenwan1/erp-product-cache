#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
if command -v node >/dev/null 2>&1; then
  CACHE_NODE="$(command -v node)"
else
  CACHE_NODE="$HOME/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin/node"
fi
[ -x "$CACHE_NODE" ] || { echo '需要 Node.js 20+' >&2; exit 2; }
export PATH="$(dirname "$CACHE_NODE"):/opt/homebrew/bin:$PATH"
CACHE_PROJECT="cache-$("$CACHE_NODE" -e "process.stdout.write(require('crypto').createHash('sha256').update(process.cwd()).digest('hex').slice(0,12))")"
compose() { docker compose -p "$CACHE_PROJECT" "$@"; }
prepare_database() {
  compose up -d --wait
  CACHE_PG_PORT="$(compose port postgres 5432 | sed 's/.*://')"
  export DATABASE_URL="postgresql://cache:cache@127.0.0.1:$CACHE_PG_PORT/cache?connection_limit=8"
  export CACHE_REDIS_PORT="$(compose port redis 6379 | sed 's/.*://')"
  "$CACHE_NODE" node_modules/prisma/build/index.js db push --skip-generate
}
case "${1:-test}" in
  install)
    if command -v pnpm >/dev/null 2>&1; then
      pnpm install --frozen-lockfile --ignore-scripts
    else
      "$CACHE_NODE" "$HOME/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/pnpm/bin/pnpm.cjs" install --frozen-lockfile --ignore-scripts
    fi
    "$CACHE_NODE" node_modules/prisma/build/index.js generate
    ;;
  build) "$CACHE_NODE" node_modules/typescript/bin/tsc -p tsconfig.json ;;
  unit) "$CACHE_NODE" node_modules/jest/bin/jest.js --runInBand --config jest.unit.cjs ;;
  test|demo)
    prepare_database
    "$CACHE_NODE" node_modules/typescript/bin/tsc -p tsconfig.json
    "$CACHE_NODE" node_modules/jest/bin/jest.js --runInBand --config jest.unit.cjs
    "$CACHE_NODE" node_modules/jest/bin/jest.js --runInBand --config jest.integration.cjs
    ;;
  up) prepare_database ;;
  down) compose down ;;
  *) echo '用法：./run.sh install|build|unit|up|test|down' >&2; exit 2 ;;
esac
