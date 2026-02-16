#!/bin/bash

# COVID-19 ETL Pipeline - Execution Script
# Can be used with cron for automated execution

# Set working directory
cd "$(dirname "$0")" || exit

# Activate virtual environment if exists
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Set timestamp
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
echo "[$TIMESTAMP] Starting COVID-19 ETL Pipeline..."

# Run pipeline
python main.py

# Check exit code
if [ $? -eq 0 ]; then
    echo "[$TIMESTAMP] ✓ Pipeline completed successfully"
    exit 0
else
    echo "[$TIMESTAMP] ✗ Pipeline failed"
    exit 1
fi