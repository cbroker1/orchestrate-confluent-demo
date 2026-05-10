#!/usr/bin/env bash
# ============================================================================
# import.sh - Import the Maintenance Triage Agent and tools into
#             watsonx Orchestrate.
#
# Prerequisites:
#   - orchestrate CLI installed and authenticated
#   - Active env set (local or remote)
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/../.env"
ORCHESTRATE="/home/cbroker/miniconda3/envs/adk/bin/orchestrate"

# Load LINEAR_API_KEY from .env
if [[ -f "$ENV_FILE" ]]; then
  LINEAR_API_KEY=$(grep '^LINEAR_API_KEY=' "$ENV_FILE" | cut -d'=' -f2-)
fi
if [[ -z "${LINEAR_API_KEY:-}" ]]; then
  echo "ERROR: LINEAR_API_KEY not found in $ENV_FILE or environment"
  exit 1
fi

echo "=== Importing tools into watsonx Orchestrate ==="

# Step 1: Import Python tools
echo "[1/6] Importing equipment_history tool..."
$ORCHESTRATE tools import --kind python --file "${SCRIPT_DIR}/tools/equipment_history.py"

echo "[2/6] Importing parts_inventory tool..."
$ORCHESTRATE tools import --kind python --file "${SCRIPT_DIR}/tools/parts_inventory.py"

echo "[3/6] Importing notify_technician tool..."
$ORCHESTRATE tools import --kind python --file "${SCRIPT_DIR}/tools/notify_technician.py"

# Step 2: Create connection with Linear API key
echo "[4/6] Creating linear-connection with API key..."
$ORCHESTRATE connections add --app-id linear-connection 2>/dev/null || true
$ORCHESTRATE connections configure \
  --app-id linear-connection \
  --env draft \
  --type team \
  --kind key_value
$ORCHESTRATE connections set-credentials \
  --app-id linear-connection \
  --env draft \
  -e "LINEAR_API_KEY=${LINEAR_API_KEY}"

# Step 3: Import Linear MCP toolkit with connection
echo "[5/6] Importing linear-mcp toolkit..."
$ORCHESTRATE toolkits add --kind mcp \
  --name linear-mcp \
  --description "Linear MCP server for creating and managing work orders" \
  --command '["npx", "-y", "@mseep/linear-mcp"]' \
  --tools "create_issue,update_issue,search_issues,get_issue" \
  --app-id linear-connection

# Step 4: Import agent
echo "[6/6] Importing maintenance-triage-agent..."
$ORCHESTRATE agents import --file "${SCRIPT_DIR}/agent/maintenance_agent.yaml"

echo ""
echo "=== Import complete ==="
echo ""
$ORCHESTRATE agents list
