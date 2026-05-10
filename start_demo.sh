#!/bin/bash
# Start all demo components

PROJECT_DIR="/home/cbroker/Documents/2026/orchestrate_confluent_demo/confluent-orchestrate-demo"
CONDA_PY="$PROJECT_DIR/../.."  # We'll use python directly

cd "$PROJECT_DIR"

echo "=== Starting Sensor Producer ==="
/home/cbroker/miniconda3/envs/orchestrate_confluent_demo/bin/python -m simulator.sensor_producer 2>&1 &
PRODUCER_PID=$!
echo "Sensor producer started (PID: $PRODUCER_PID)"

echo "=== Starting Streamlit Dashboard ==="
/home/cbroker/miniconda3/envs/orchestrate_confluent_demo/bin/streamlit run dashboard/app.py --server.port 8501 --server.headless true --browser.gatherUsageStats false 2>&1 &
DASHBOARD_PID=$!
echo "Dashboard started (PID: $DASHBOARD_PID)"

echo ""
echo "Demo is running!"
echo "  Producer PID: $PRODUCER_PID"  
echo "  Dashboard: http://localhost:8501"
echo ""
