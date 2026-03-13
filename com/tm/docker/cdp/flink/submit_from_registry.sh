#!/usr/bin/env bash
# =============================================================================
# submit_from_registry.sh — Generate + submit Flink SQL job from event_registry.yml
# =============================================================================
# Usage:
#   ./flink/submit_from_registry.sh <source_id>
#   ./flink/submit_from_registry.sh login
#
# Prerequisites:
#   - python3, pyyaml (pip install pyyaml)
#   - Docker containers: cdp-flink-jobmanager running
#   - Run from: com/tm/docker/cdp/
# =============================================================================

set -euo pipefail

SOURCE_ID="${1:-}"
if [ -z "$SOURCE_ID" ]; then
    echo "Usage: $0 <source_id>" >&2
    echo "Available sources:" >&2
    python3 - <<'EOF'
import yaml, sys
with open("registry/event_registry.yml") as f:
    reg = yaml.safe_load(f)
for s in reg['sources']:
    status = "enabled" if s.get('enabled', True) else "disabled"
    print(f"  {s['source_id']:20s} [{status}]  {s.get('description','')}")
EOF
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REGISTRY="$SCRIPT_DIR/../registry/event_registry.yml"
TEMPLATE="$SCRIPT_DIR/templates/kafka_to_iceberg.sql.tmpl"
OUTPUT="/tmp/cdp_flink_${SOURCE_ID}.sql"

echo "==> Generating Flink SQL for source: $SOURCE_ID"

python3 - "$SOURCE_ID" "$REGISTRY" "$TEMPLATE" "$OUTPUT" <<'PYEOF'
import sys, yaml

source_id, registry_path, template_path, output_path = sys.argv[1:]

with open(registry_path) as f:
    registry = yaml.safe_load(f)

source = next((s for s in registry['sources'] if s['source_id'] == source_id), None)
if source is None:
    print(f"ERROR: source_id '{source_id}' not found in registry", file=sys.stderr)
    sys.exit(1)
if not source.get('enabled', True):
    print(f"WARNING: source '{source_id}' is disabled in registry", file=sys.stderr)
    sys.exit(1)

# Build field definition strings
kafka_fields = []
iceberg_fields = []
field_names = []
for f in source['fields']:
    name    = f['name']
    ftype   = f['type']
    comment = f.get('comment', '')
    kafka_fields.append(f"    {name:<16} {ftype:<10}" + (f' COMMENT \'{comment}\'' if comment else ''))
    iceberg_fields.append(f"    {name:<16} {ftype:<10}" + (f' COMMENT \'{comment}\'' if comment else ''))
    field_names.append(f"    {name}")

with open(template_path) as f:
    sql = f.read()

sql = sql.replace('__SOURCE_ID__',              source['source_id'])
sql = sql.replace('__KAFKA_TOPIC__',            source['kafka_topic'])
sql = sql.replace('__CONSUMER_GROUP__',         source['flink_consumer_group'])
sql = sql.replace('__ICEBERG_DB__',             source['iceberg_database'])
sql = sql.replace('__ICEBERG_TABLE__',          source['iceberg_table'])
sql = sql.replace('__PARTITION_FIELD__',        source['partition_field'])
sql = sql.replace('__KAFKA_FIELD_DEFINITIONS__', ',\n'.join(kafka_fields))
sql = sql.replace('__ICEBERG_FIELD_DEFINITIONS__', ',\n'.join(iceberg_fields))
sql = sql.replace('__FIELD_NAMES__',            ',\n'.join(field_names))

with open(output_path, 'w') as f:
    f.write(sql)

print(f"Generated: {output_path}")
PYEOF

echo "==> Copying SQL to Flink JobManager container"
docker cp "$OUTPUT" cdp-flink-jobmanager:/tmp/cdp_flink_${SOURCE_ID}.sql

echo "==> Submitting job to Flink SQL Client"
docker exec cdp-flink-jobmanager \
    ./bin/sql-client.sh -f /tmp/cdp_flink_${SOURCE_ID}.sql

echo "==> Done. Check Flink UI: http://localhost:8085"
PYEOF
chmod +x /Users/toanmai/Documents/code/thor/com/tm/docker/cdp/flink/submit_from_registry.sh