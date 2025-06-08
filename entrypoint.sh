#!/bin/bash
echo "Installing dependencies..."
pip install --no-cache-dir -r /requirements.txt

echo "Starting Streamlit dashboard..."
streamlit run /opt/airflow/Visor/dashboard.py --server.port 8501 --server.enableCORS false
