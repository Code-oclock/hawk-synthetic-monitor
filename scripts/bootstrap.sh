#!/usr/bin/env bash
# Prepares a Hawk project for the monitor and writes the HAWK_* settings to an env file.
# Idempotent: with an existing OUT file it reuses the project and only makes sure the webhook rule exists.
#
# Needs: curl, python3. Mongo mode also needs docker (the Hawk compose project with a `mongodb` service).
#
#   WEBHOOK_MODE=mongo  local Hawk: the rule is written straight to MongoDB (the API refuses private URLs)
#   WEBHOOK_MODE=api    any Hawk: the rule is created through the API (WEBHOOK_URL must be public, port 80/443)
set -euo pipefail

API=${API:-http://localhost:4000/graphql}
COLLECTOR=${COLLECTOR:-http://localhost:3000/}
EMAIL=${EMAIL:-monitor@example.com}
OUT=${OUT:-.env}
WEBHOOK_MODE=${WEBHOOK_MODE:-mongo}
WEBHOOK_URL=${WEBHOOK_URL:-http://host.docker.internal:8085/webhook}
HAWK_COMPOSE_DIR=${HAWK_COMPOSE_DIR:-}
# Addresses written to the env file for the monitor (it usually runs in Docker).
ENV_COLLECTOR=${ENV_COLLECTOR:-http://host.docker.internal:3000/}
ENV_API=${ENV_API:-http://host.docker.internal:4000/graphql}

log() { echo "[bootstrap] $*" >&2; }
die() { log "$*"; exit 1; }

gql() { # gql <token|-> <json-body>
  local auth="X-No-Auth: 1"
  [ "$1" != "-" ] && auth="Authorization: Bearer $1"
  curl -sS -m 20 "$API" -H 'content-type: application/json' -H "$auth" -d "$2"
}
field() { # field <python-expr over d>; prints the error body when GraphQL returned errors
  python3 -c 'import sys,json; d=json.load(sys.stdin)
if "errors" in d: print(json.dumps(d["errors"]), file=sys.stderr); sys.exit(1)
print(eval(sys.argv[1]))' "$1"
}
envget() { grep "^$1=" "$OUT" 2>/dev/null | head -1 | cut -d= -f2- || true; }

command -v curl >/dev/null && command -v python3 >/dev/null || die "curl and python3 are required"
curl -fsS -m 5 "${COLLECTOR%/}/health" >/dev/null || die "collector is not reachable at $COLLECTOR"

PID=$(envget HAWK_PROJECT_ID)
if [ -n "$PID" ]; then
  log "reusing project $PID from $OUT"
  PASS=$(envget HAWK_PASSWORD)
  ITOKEN=$(envget HAWK_INTEGRATION_TOKEN)
  EMAIL=$(envget HAWK_EMAIL)
  TOKEN=$(gql - "{\"query\":\"mutation(\$e:String!,\$p:String!){login(email:\$e,password:\$p){accessToken}}\",\"variables\":{\"e\":\"$EMAIL\",\"p\":\"$PASS\"}}" | field 'd["data"]["login"]["accessToken"]')
else
  [ -s "$OUT" ] && die "$OUT exists but has no HAWK_PROJECT_ID: refusing to overwrite it (move it away or add the missing values)"
  log "creating user $EMAIL (needs the API in NODE_ENV=e2e: signUp returns the password)"
  R=$(gql - "{\"query\":\"mutation(\$e:String!){signUp(email:\$e)}\",\"variables\":{\"e\":\"$EMAIL\"}}")
  PASS=$(echo "$R" | field 'd["data"]["signUp"]') || die "signUp failed (user exists already? delete it or set EMAIL): $R"
  TOKEN=$(gql - "{\"query\":\"mutation(\$e:String!,\$p:String!){login(email:\$e,password:\$p){accessToken}}\",\"variables\":{\"e\":\"$EMAIL\",\"p\":\"$PASS\"}}" | field 'd["data"]["login"]["accessToken"]')
  WS=$(gql "$TOKEN" '{"query":"mutation{createWorkspace(name:\"Synthetic\"){id}}"}' | field 'd["data"]["createWorkspace"]["id"]')
  PR=$(gql "$TOKEN" "{\"query\":\"mutation{createProject(workspaceId:\\\"$WS\\\",name:\\\"Synthetic Monitor\\\"){id token}}\"}")
  PID=$(echo "$PR" | field 'd["data"]["createProject"]["id"]')
  ITOKEN=$(echo "$PR" | field 'd["data"]["createProject"]["token"]')
  umask 077
  : > "$OUT" && chmod 600 "$OUT"
  { echo "HAWK_COLLECTOR_URL=$ENV_COLLECTOR"; echo "HAWK_API_URL=$ENV_API"
    echo "HAWK_PROJECT_ID=$PID"; echo "HAWK_INTEGRATION_TOKEN=$ITOKEN"
    echo "HAWK_EMAIL=$EMAIL"; echo "HAWK_PASSWORD=$PASS"; } > "$OUT"
  log "project $PID created, settings saved to $OUT (mode 600)"
fi

has_rule() {
  gql "$TOKEN" "{\"query\":\"{project(projectId:\\\"$PID\\\"){notifications{channels{webhook{endpoint}}}}}\"}" \
    | WEBHOOK_URL="$WEBHOOK_URL" python3 -c 'import sys,json,os
d=json.load(sys.stdin)
rules=((d.get("data") or {}).get("project") or {}).get("notifications") or []
print(any(((r.get("channels") or {}).get("webhook") or {}).get("endpoint")==os.environ["WEBHOOK_URL"] for r in rules))'
}

if [ "$(has_rule 2>/dev/null || echo False)" = "True" ]; then
  log "webhook rule for $WEBHOOK_URL exists"
elif [ "$WEBHOOK_MODE" = "api" ]; then
  log "creating webhook rule through the API"
  gql "$TOKEN" "{\"query\":\"mutation(\$i:CreateProjectNotificationsRuleInput!){createProjectNotificationsRule(input:\$i){id}}\",\"variables\":{\"i\":{\"projectId\":\"$PID\",\"whatToReceive\":\"ONLY_NEW\",\"channels\":{\"webhook\":{\"endpoint\":\"$WEBHOOK_URL\",\"minPeriod\":0}}}}}" \
    | field 'd["data"]["createProjectNotificationsRule"]["id"]' >/dev/null \
    || die "API rejected the rule: the URL must be public http(s) on port 80 or 443; use WEBHOOK_MODE=mongo on a local Hawk"
else
  [ -n "$HAWK_COMPOSE_DIR" ] || die "set HAWK_COMPOSE_DIR to the local Hawk checkout (for docker compose exec mongodb)"
  log "writing the webhook rule to MongoDB"
  (cd "$HAWK_COMPOSE_DIR" && docker compose exec -T mongodb mongosh --quiet hawk --eval "
    db.projects.updateOne({_id: ObjectId('$PID')}, {\$push: {notifications: {
      _id: new ObjectId(), isEnabled: true, whatToReceive: 'ONLY_NEW', including: [], excluding: [],
      channels: {webhook: {isEnabled: true, endpoint: '$WEBHOOK_URL', minPeriod: 0}}}}})" >/dev/null)
fi

log "waiting for the collector to accept the new token (its cache refreshes every ~30 s)"
for i in $(seq 1 30); do
  code=$(curl -s -m 10 -o /dev/null -w '%{http_code}' "$COLLECTOR" -H 'content-type: application/json' \
    -d "{\"token\":\"$ITOKEN\",\"catcherType\":\"errors/golang\",\"payload\":{\"title\":\"bootstrap smoke $(date +%s)\",\"type\":\"error\",\"backtrace\":[{\"file\":\"main.go\",\"line\":1,\"function\":\"main\"}]}}")
  [ "$code" = "200" ] && { log "ready: the collector accepts events (note: the notifier caches rules for up to 60 s)"; exit 0; }
  sleep 3
done
die "collector kept rejecting the token (last HTTP $code)"
