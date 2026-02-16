#!/bin/bash

# COVID-19 ETL Pipeline - Setup Script
# Automated installation and configuration

set -e

echo "🦠 COVID-19 ETL Pipeline - Setup"
echo "=================================="
echo ""

# Check Python version
echo "Checking Python version..."
PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
REQUIRED_VERSION="3.9"

if [ "$(printf '%s\n' "$REQUIRED_VERSION" "$PYTHON_VERSION" | sort -V | head -n1)" != "$REQUIRED_VERSION" ]; then 
    echo "❌ Error: Python 3.9+ is required. Found: $PYTHON_VERSION"
    exit 1
fi

echo "✅ Python $PYTHON_VERSION detected"
echo ""

# Check Java (for PySpark)
echo "Checking Java installation..."
if ! command -v java &> /dev/null; then
    echo "⚠️  Java not found. PySpark requires Java 8 or 11."
    echo "   Please install Java: sudo apt-get install openjdk-17-jre"
else
    JAVA_VERSION=$(java -version 2>&1 | head -n 1 | awk -F '"' '{print $2}')
    echo "✅ Java $JAVA_VERSION detected"
fi
echo ""

# Create virtual environment
echo "Creating virtual environment..."
if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo "✅ Virtual environment created"
else
    echo "ℹ️  Virtual environment already exists"
fi
echo ""

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate
echo "✅ Virtual environment activated"
echo ""

# Upgrade pip
echo "Upgrading pip..."
pip install --upgrade pip > /dev/null 2>&1
echo "✅ Pip upgraded"
echo ""

# Install dependencies
echo "Installing dependencies..."
echo "This may take several minutes (PySpark is large)..."
pip install -r requirements.txt > /dev/null 2>&1
echo "✅ Dependencies installed"
echo ""

# Create directories
echo "Creating data directories..."
mkdir -p data/raw data/processed data/database logs
echo "✅ Directories created"
echo ""

# Run tests
echo "Running tests..."
pytest tests/ -v --tb=short
TEST_RESULT=$?

if [ $TEST_RESULT -eq 0 ]; then
    echo "✅ All tests passed"
else
    echo "⚠️  Some tests failed. Check output above."
fi
echo ""

# Setup complete
echo "=================================="
echo "✅ Setup complete!"
echo ""
echo "To run the pipeline:"
echo "  source venv/bin/activate"
echo "  python main.py"
echo ""
echo "To run the dashboard:"
echo "  streamlit run src/visualization/dashboard.py"
echo ""
echo "To use Docker:"
echo "  docker-compose -f docker/docker-compose.yml up"
echo ""
echo "Happy analyzing! 🦠📊"