#!/bin/bash

echo "======================================================================"
echo "  QUICK INTEGRATION TEST - Updated Architecture Validation"
echo "======================================================================"
echo ""

# Check prerequisites
echo "🔍 Checking prerequisites..."

# Check Python dependencies
echo "📦 Checking Python packages..."
python3 -c "import clickhouse_driver; import requests; import pandas" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "⚠️  Installing required packages..."
    pip3 install clickhouse-driver requests pandas
fi

# Check ClickHouse
echo "🗄️  Checking ClickHouse..."
nc -z localhost 9000 2>/dev/null
if [ $? -eq 0 ]; then
    echo "✅ ClickHouse is running"
else
    echo "❌ ClickHouse is not running on port 9000"
    echo "   Start with: cd /Users/nguyenvietkhoi/VinaTien/ML && docker-compose up -d clickhouse"
    exit 1
fi

# Check backend (optional)
echo "🔧 Checking backend..."
nc -z localhost 8000 2>/dev/null
if [ $? -eq 0 ]; then
    echo "✅ Backend is running"
else
    echo "⚠️  Backend is not running (VPBank test may fail)"
fi

echo ""
echo "======================================================================"
echo "  RUNNING INTEGRATION TESTS"
echo "======================================================================"
echo ""

# Run the Python test suite
cd /Users/nguyenvietkhoi/VinaTien/ML
python3 test_integration.py

exit_code=$?

echo ""
echo "======================================================================"
if [ $exit_code -eq 0 ]; then
    echo "  ✅ ALL TESTS PASSED!"
    echo "  Your updated architecture with ReplacingMergeTree is working!"
else
    echo "  ❌ SOME TESTS FAILED"
    echo "  Check the output above for details"
fi
echo "======================================================================"

exit $exit_code
