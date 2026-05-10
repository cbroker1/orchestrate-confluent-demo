# Predictive Maintenance Demo: Confluent Cloud + IBM watsonx Orchestrate
*Original demo and infrastructure by [Sean Falconer](https://github.com/thefalc). See his work at [confluent-orchestrate-demo](https://github.com/thefalc/confluent-orchestrate-demo) — he built this entire demo. My contributions were limited to adding the Linear MCP tools and simplifying the Linear setup steps in the ADK.*

An end-to-end demo that streams IoT sensor data through Confluent Cloud, detects anomalies with Flink SQL's built-in ML, and triggers an AI agent in IBM watsonx Orchestrate to triage issues and create Linear work orders.

## Architecture

```
Sensor Simulator ──► Kafka (sensor-readings) ──► Flink SQL (ML_DETECT_ANOMALIES)
                              │                            │
                              │                            ▼
                              │                  Kafka (equipment-alerts)
                              │                     │              │
                              ├───────────────────────┘              │
                              ▼                                    ▼
                     Streamlit Dashboard                  HTTP Sink Connector
                                                                     │
                                                                     ▼
                                                               ngrok tunnel
                                                                     │
                                                                     ▼
                                                              Webhook Proxy
                                                                     │
                                                                     ▼
                                                               Orchestrate Agent
                                                                     │
                                                    ┌─────────┬──────┼───────┬──────────┐
                                                    ▼         ▼      ▼       ▼          ▼
                                               Check History  Check Parts  Create Linear  Notify Tech
                                                                     Issue
```

**Data flow:**
1. **Simulator** generates synthetic sensor readings (vibration, temperature, pressure) for 4 industrial machines
2. **Confluent Cloud Kafka** ingests readings into the `sensor-readings` topic (JSON Schema Registry format)
3. **Flink SQL** runs `ML_DETECT_ANOMALIES` over 10-second tumbling windows using ARIMA modeling
4. Detected anomalies are written to the `equipment-alerts` topic
5. **HTTP Sink Connector** forwards alerts to a local webhook proxy via an ngrok tunnel
6. **Webhook proxy** invokes the Orchestrate agent via the Runs API
7. The **watsonx Orchestrate agent** triages the alert:
   - Assesses severity (CRITICAL / HIGH / MEDIUM / LOW based on anomaly score)
   - Looks up equipment maintenance history
   - Checks parts inventory and availability
   - Creates a Linear issue with structured description and priority
   - Notifies the assigned technician
8. **Streamlit dashboard** provides real-time visualization and anomaly injection controls

## Project Structure

```
├── simulator/                  # Sensor data generation
│   ├── config.py               # Kafka config, machine & sensor definitions
│   ├── sensor_producer.py      # Produces readings to Kafka (every 10s)
│   └── anomaly_injector.py     # CLI to inject controlled anomalies
├── flink/                      # Flink SQL jobs (run in Confluent Cloud)
│   ├── 01_sensor_table.sql     # Add event_time column & watermark
│   ├── 02_anomaly_detection.sql # ML-based anomaly detection pipeline
│   └── 03_alerts_table.sql     # equipment-alerts sink table
├── connector/                  # Alert routing to Orchestrate
│   ├── http_sink_config.json   # HTTP Sink Connector config
│   ├── webhook_proxy.py        # Flask server -> Orchestrate Runs API
│   └── alert_consumer.py       # Alternative: Kafka consumer -> CLI
├── orchestrate/                # watsonx Orchestrate agent, tools & MCP
│   ├── agent/
│   │   └── maintenance_agent.yaml  # Agent spec with triage instructions
│   ├── tools/
│   │   ├── equipment_history.py    # Maintenance history lookup
│   │   ├── parts_inventory.py      # Parts availability check
│   │   └── notify_technician.py    # Slack/log notification
│   ├── toolkits/
│   │   └── linear_mcp_config.json  # Linear MCP server config
│   └── import.sh               # Import agent & tools into Orchestrate
├── dashboard/
│   └── app.py                  # Streamlit real-time monitoring UI
├── demo/                       # Demo lifecycle scripts
│   ├── setup_confluent.sh      # Create Kafka topics
│   ├── run_demo.sh             # Start producer + dashboard
│   └── reset_demo.sh           # Clear state and stop services
├── docker-compose.yml          # Local Orchestrate server deployment
└── server.env                  # Orchestrate server config
```

## Prerequisites

### External Accounts & Subscriptions

| Service | Purpose | What You Need |
|---------|---------|---------------|
| [IBM Cloud](https://cloud.ibm.com/) | Watsonx Orchestrate Developer Edition | IBM Cloud account (free tier works) |
| [Confluent Cloud](https://confluent.cloud/) | Kafka cluster + Flink SQL | Free or Pro account |
| [Linear](https://linear.app/) | Work order creation | Free organization + API key |
| [ngrok](https://ngrok.com/) | Expose local webhook proxy | Free account + authtoken |
| [Docker](https://docker.com/) | Run Orchestrate Developer Edition | Docker Desktop or Docker Engine |
| [Conda](https://anaconda.com/) | Python environment for Orchestrate CLI | Miniconda |

### Local Software

| Software | Required Version | Purpose |
|----------|-----------------|---------|
| Python | 3.10+ | Dependencies (flask, requests, etc.) |
| Node.js | 18+ | Runs `@mseep/linear-mcp` |
| ngrok CLI | Latest | HTTP tunneling |
| confluent CLI | Latest | Confluent Cloud management |

---

## Part 1: Provision IBM Watsonx Orchestrate (Developer Edition)

### Option A: Via IBM Cloud / TechZone

1. Go to the [IBM Cloud Catalog](https://cloud.ibm.com/catalog/services/watsonx-orchestrate)
2. Search for **"watsonx Orchestrate"** or **"watsonx Orchestrate Developer Edition"**
3. Click **"Create"** to provision your instance
4. Wait for the instance to be ready (normally 5-10 minutes)
5. Once ready, note down:
   - **Instance URL** (format: `https://api.us-south.watson-orchestrate.cloud.ibm.com/instances/<UUID>`)
   - **API Key** (from IAM -> API Keys)

### Option B: Local Developer Edition

For local development, install the Watsonx Orchestrate Developer Edition:

```bash
# 1. Install the orchestrate CLI
pip install watsonx-orchestrate
# OR use the downloaded binary:
# chmod +x orchestrate && mv orchestrate /usr/local/bin/

# 2. Log in
orchestrate env configure --type dev-edition

# 3. Accept the Developer Edition source:
export WO_DEVELOPER_EDITION_SOURCE=orchestrate

# 4. Activate your environment
orchestrate env activate local
```

---

## Part 2: Confluent Cloud Setup

### Step 1: Create a Confluent Cloud Account

1. Go to https://confluent.cloud/ and sign up
2. Create a **Freemium** or **Pro** cluster
3. Go to **Environment** settings and note your Environment ID (e.g., `env-p62vvm`)

### Step 2: Get API Credentials

**Kafka API Credentials:**
1. Navigate to **Platform API Keys** in Confluent Cloud
2. Create two keys: one for Kafka, one for Schema Registry
3. Save them securely

**Cloud REST API Credentials:**
1. Go to **Account Settings** -> **Cloud Account API Keys**
2. Create a Cloud API key (used for connector management)

### Step 3: Create a Flink Compute Pool

1. Go to **Flink** in the side nav
2. Click **Create Compute Pool**
3. Name it (e.g., `demo-flink-pool`)
4. Note the Compute Pool ID (e.g., `lfcp-yz17xo`)
5. Pick a region (e.g., `us-east-2`)

### Step 4: Choose Your Setup Method

#### Option A: Manual Setup (UI-Based)

1. **Create Topics:**
   - In Confluent Cloud, go to **Clusters** -> your cluster -> **Topics**
   - Click **Create Topic**: name `sensor-readings`, partitions 4, retention 1 day
   - Click **Create Topic**: name `equipment-alerts`, partitions 4, retention 1 day

2. **Set up Flink SQL:**
   - Go to **Flink** workspace
   - Run `flink/01_sensor_table.sql` — adds `event_time` watermarked column
   - Run `flink/03_alerts_table.sql` — creates `equipment-alerts` sink table
   - Run `flink/02_anomaly_detection.sql` — deploys anomaly detection pipeline

3. **Deploy HTTP Sink Connector:**
   - Go to **Clusters** -> your cluster -> **Connectors**
   - Click **Create Connector**, search for **HTTP Sink**
   - Paste the JSON from `connector/http_sink_config.json`
   - **IMPORTANT:** Replace the `http.api.url` with your ngrok URL later

#### Option B: Script-Based Setup (Recommended)

```bash
# After filling in .env (see Part 4), run:
./demo/setup_confluent.sh
```

This script:
- Authenticates your Confluent CLI
- Sets the active environment and cluster
- Creates `sensor-readings` and `equipment-alerts` topics (if they don't exist)
- Verifies the setup

---

## Part 3: Linear Account Setup

1. Go to https://linear.app/ and create an organization (or use an existing one)
2. Go to **Settings -> Developers** (or https://linear.app/settings/developer)
3. Click **Create New API Key**
4. Copy the key — it looks like: `lin_api_xxxx...`
5. **Note:** The Linear MCP server uses this key to create/manage issues

---

## Part 4: ngrok Setup

```bash
# Install ngrok if you haven't
brew install ngrok              # macOS
# OR see https://ngrok.com/download for other platforms

# Login with your authtoken (from https://dashboard.ngrok.com/get-started/your-authtoken)
ngrok config add-authtoken YOUR_AUTHTOKEN_HERE

# Verify it's configured
ngrok authtoken show
```

---

## Part 5: Orchestrate Developer Edition Install & Conda Environment

### Step 1: Create the Conda Environment

```bash
# Create a dedicated conda environment
conda create -n orchestrate_confluent_demo python=3.12 -y
conda activate orchestrate_confluent_demo

# Install the Orchestrate CLI
pip install watsonx-orchestrate

# Verify installation
orchestrate --help
```

### Step 2: Download and Start the Watsonx Orchestrate Server

```bash
# The Orchestrate server runs via Docker Compose (defined in docker-compose.yml)
# Navigate to the project directory:
cd confluent-orchestrate-demo

# The server.env file contains all Orchestrate server settings
# (generated by the orchestrate CLI during setup)

# Start the server:
orchestrate server start --env-file server.env

# OR if using docker-compose directly:
docker-compose up -d

# Wait for all containers to become healthy (especially wxo-server)
# Verify the API is responding:
curl http://localhost:4321/health
```

### Step 3: Activate Environment for CLI Access

```bash
orchestrate env activate local
```

---

## Part 6: Populate .env File

Create `.env` in the project root with all credentials:

```bash
# Copy the template
cp .env.example .env

# Edit .env and fill in ALL values:
```

**Required `.env` fields:**

```bash
# === Confluent Cloud ===
CONFLUENT_BOOTSTRAP_SERVERS=pkc-xxx.us-east-2.aws.confluent.cloud:9092
CONFLUENT_API_KEY=YOUR_KAFKA_API_KEY
CONFLUENT_API_SECRET=YOUR_KAFKA_API_SECRET
CONFLUENT_SCHEMA_REGISTRY_URL=https://psrc-xxx.us-east-2.aws.confluent.cloud
CONFLUENT_SR_API_KEY=YOUR_SR_API_KEY
CONFLUENT_SR_API_SECRET=YOUR_SR_API_SECRET
CONFLUENT_CLOUD_API_KEY=YOUR_CLOUD_API_KEY
CONFLUENT_CLOUD_API_SECRET=YOUR_CLOUD_API_SECRET
CONFLUENT_ENVIRONMENT_ID=env-xxxxx
CONFLUENT_CLUSTER_ID=lkc-xxxxx

# === watsonx Orchestrate (Cloud) ===
WXO_API_KEY=YOUR_WATSONX_API_KEY
WXO_INSTANCE_URL=https://api.us-south.watson-orchestrate.cloud.ibm.com/instances/YOUR_INSTANCE_UUID

# === Orchestrate Developer Edition (Local) ===
ORCHESTRATE_LOCAL_URL=http://localhost:4321
ORCHESTRATE_AGENT_ID=YOUR_AGENT_ID_FROM_IMPORT

# === JWT (from local Orchestrate server.env) ===
JWT_SECRET=YOUR_JWT_SECRET_FROM_SERVER_ENV
DEFAULT_TENANT_ID=10000000-0000-0000-0000-000000000000

# === Linear MCP ===
LINEAR_API_KEY=lin_api_XXXXXXXXX

# === Developer Edition Source ===
WO_DEVELOPER_EDITION_SOURCE=orchestrate

# === Slack (optional) ===
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/xxx/xxx/xxx
```

---

## Part 7: Install All Dependencies

```bash
# Activate conda env first
conda activate orchestrate_confluent_demo

# Install Python dependencies
pip install flask PyJWT attrs python-dotenv requests streamlit confluent-kafka

# (confluent CLI is system-wide, not pip-installed — see Prerequisites)
```

---

## Part 8: Import MCP Tools and Agent into Orchestrate

### Step 1: Install Node.js MCP Server

```bash
# The Linear MCP server runs as an npx package
# Make sure Node.js >= 18 is installed:
node --version

# No manual install needed — @mseep/linear-mcp is fetched via npx at runtime
```

### Step 2: Import Linear MCP Toolkit with Connection

The Linear MCP toolkit needs a **connection** in Orchestrate to hold the API key:

```bash
orchestrate connections add --app-id linear-connection

orchestrate connections configure \
  --app-id linear-connection \
  --env draft \
  --type team \
  --kind key_value

orchestrate connections set-credentials \
  --app-id linear-connection \
  --env draft \
  -e "LINEAR_API_KEY=lin_api_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"

orchestrate toolkits add --kind mcp \
  --name linear-mcp \
  --description "Linear MCP server for creating and managing work orders" \
  --command '["npx", "-y", "@mseep/linear-mcp"]' \
  --tools "create_issue,update_issue,search_issues,get_issue" \
  --app-id linear-connection
```

Verify it's registered:

```bash
orchestrate toolkits list
# Should show linear-mcp with create_issue, list_issues, update_issue, etc.
```

### Step 3: Import Python Tools

```bash
orchestrate tools import --kind python --file orchestrate/tools/equipment_history.py
orchestrate tools import --kind python --file orchestrate/tools/parts_inventory.py
orchestrate tools import --kind python --file orchestrate/tools/notify_technician.py
```

### Step 4: Import the Agent

```bash
orchestrate agents import --file orchestrate/agent/maintenance_agent.yaml
```

Verify the agent is registered:

```bash
orchestrate agents list
# Should show maintenance_triage_agent with tools: equipment_history, parts_inventory, notify_technician, linear-mcp:create_issue
```

### Step 5: Get Agent ID and Update .env

```bash
orchestrate agents list
# Copy the agent UUID from the output and set ORCHESTRATE_AGENT_ID in .env
```

---

## Part 9: Start the Full Pipeline

### Step 1: Start ngrok Tunnel

```bash
ngrok http 8090
# Note the URL it returns, e.g., https://790b-xxxx.ngrok-free.app
```

### Step 2: Deploy HTTP Sink Connector

In Confluent Cloud:
1. Go to **Clusters** -> your cluster -> **Connectors**
2. Click **Create Connector**, search for **HTTP Sink**
3. Set `topics` to `equipment-alerts`
4. Set `http.api.url` to your ngrok URL + `/alert` (e.g., `https://790b-xxxx.ngrok-free.app/alert`)
5. Set `input.data.format` to `JSON_SR` (not `JSON` — because Flink writes with `json-registry` format)
6. Set `request.body.format` to `json`
7. Set `batch.max.size` to `1`

### Step 3: Deploy Flink SQL Jobs

In the Confluent Cloud Flink SQL workspace, execute in order:

1. **flink/01_sensor_table.sql** — adds `event_time` watermarked column to `sensor-readings`
2. **flink/03_alerts_table.sql** — creates `equipment-alerts` sink table
3. **flink/02_anomaly_detection.sql** — deploys the anomaly detection pipeline

> **Note:** The anomaly detection uses `minTrainingSize=30`. Flink needs ~5 minutes of baseline sensor data before it can detect anomalies.

### Step 4: Start Services

```bash
# Terminal 1: Start webhook proxy
python connector/webhook_proxy.py
# Listens on localhost:8090

# Terminal 2: Start the sensor producer
python -m simulator.sensor_producer
# Sends readings every 10 seconds

# Terminal 3: Start the Streamlit dashboard
streamlit run dashboard/app.py --server.port 8501 --server.headless true
# Opens at http://localhost:8501
```

Or use the launch script:

```bash
./demo/run_demo.sh
```

---

## Part 10: Test the End-to-End Flow

### Method A: Via Streamlit Dashboard (Easy)

1. Open **http://localhost:8501**
2. Use the sidebar controls to select a machine and sensor
3. Click **"Inject Anomaly"** to trigger degradation
4. Wait ~5 minutes for Flink to warm up the ML model and detect the anomaly
5. You should see:
   - Alert appear in `equipment-alerts` topic (check in Confluent Cloud UI)
   - Linear issue created in your Linear workspace
   - Technician notification sent

### Method B: Via CLI (For Testing)

```bash
# Inject an anomaly via CLI
python -m simulator.anomaly_injector compressor-01 vibration --intensity 5.0

# OR manually invoke the agent via Orchestrate CLI:
orchestrate env activate local
orchestrate agents chat <YOUR_AGENT_ID>

# Paste a JSON payload from the equipment-alerts topic:
# (Get it from Confluent Cloud -> Topics -> equipment-alerts -> Messages tab)
```

### Method C: Manual Agent Invocation (Full Debug)

1. Go to **Confluent Cloud** -> **Topics** -> `equipment-alerts` -> **Messages**
2. Copy the JSON payload from the Message details
3. Go to your **Orchestrate agent chat** (via `orchestrate agents chat` or the UI at `http://localhost:3000/chat-lite`)
4. Paste the JSON payload into the chat window
5. The agent will:
   - Extract machine_id, sensor_type, anomaly_score, value
   - Look up maintenance history
   - Check parts inventory
   - Create a Linear work order
   - Notify technician
6. Verify in your **Linear workspace** for the created issue with:
   - Title: `[SEVERITY] sensor_type anomaly on machine_id`
   - Structured description with Anomaly Detected, Maintenance History, Parts Status, and Action Required sections
   - Priority set based on severity (CRITICAL=1, HIGH=2, MEDIUM=3, LOW=4)

---

## Part 11: Stop the Demo

```bash
# Stop all services
pkill -f sensor_producer
pkill -f webhook_proxy
pkill -f streamlit

# Remove any active anomaly state
rm -f /tmp/anomaly_target.json

# Or use the reset script:
./demo/reset_demo.sh

# Stop ngrok
# (Ctrl+C in the ngrok terminal)
```

---

## Machines & Sensors

| Machine | Facility | Criticality | Normal Temp | Normal Vibration | Normal Pressure |
|---------|----------|-------------|-------------|-----------------|-----------------|
| compressor-01 | plant-north | high | ~72°C | ~3.5 mm/s | ~150 psi |
| compressor-02 | plant-north | medium | ~72°C | ~3.5 mm/s | ~150 psi |
| pump-03 | plant-south | high | ~72°C | ~3.5 mm/s | ~150 psi |
| turbine-04 | plant-south | critical | ~72°C | ~3.5 mm/s | ~150 psi |

---

## Troubleshooting

### Connector returns errors
- Ensure the ngrok tunnel is running and the URL in the connector config matches
- Ensure `input.data.format` is set to `JSON_SR` (not `JSON`)
- Verify `http.api.url` includes `/alert` path (e.g., `https://xxx.ngrok.app/alert`)

### Linear issue creation fails
- Run `orchestrate connections set-credentials` again to verify LINEAR_API_KEY is set
- Verify the Linear API key is valid in https://linear.app/settings/developer

### Dashboard shows UnicodeDecodeError
- The dashboard strips the 5-byte Schema Registry header from messages
- Ensure you're running the latest version of `dashboard/app.py`

### Flink not detecting anomalies
- The ML model needs ~30 data points (~5 minutes at 10-second intervals) of baseline data first
- Let the sensor producer run for a few minutes before injecting an anomaly
- Check Flink SQL job status in Confluent Cloud

### Agent not responding to webhook
- Verify webhook proxy is running on port 8090
- Check Orchestrate server health: `curl http://localhost:4321/health`
- Verify agent ID in `.env` matches the imported agent
- Check that the Linear MCP connection has credentials set (`orchestrate connections list`)

### ngrok connection drops
- Free ngrok tunnels disconnect periodically
- Restart ngrok: `ngrok http 8090`
- Update the HTTP Sink Connector URL in Confluent Cloud to the new ngrok URL

### Confluent CLI auth expired
- Run `confluent login --save` to re-authenticate
- Then verify with `confluent kafka cluster list`

---

## License

Apache License 2.0
