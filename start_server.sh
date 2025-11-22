#!/bin/bash

# AutoETL Server Startup Script
# This script starts the FastAPI backend server

echo "🚀 Starting AutoETL Server..."
echo "================================"

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "❌ Virtual environment not found!"
    echo "Please run: python -m venv venv"
    echo "Then: source venv/bin/activate"
    echo "And: pip install -r requirements.txt"
    exit 1
fi

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo "⚠️  Warning: .env file not found!"
    echo "Please create .env from .env.example"
    echo "Example: cp .env.example .env"
    echo ""
    read -p "Continue anyway? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Activate virtual environment
echo "📦 Activating virtual environment..."
source venv/bin/activate

# Check if uvicorn is installed
if ! command -v uvicorn &> /dev/null; then
    echo "❌ uvicorn not found! Installing..."
    pip install uvicorn fastapi
fi

# Start the API server
echo ""
echo "✅ Starting API Server on http://localhost:8000"
echo "📊 API Documentation: http://localhost:8000/docs"
echo "🌐 Web Interface: Open web/index.html in your browser"
echo ""
echo "Press Ctrl+C to stop the server"
echo "================================"
echo ""

cd "$(dirname "$0")"
python -m uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
