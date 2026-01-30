#!/bin/bash
# Quick start script for VinaTien ML API

set -e

echo "🚀 Starting VinaTien ML API Service..."
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

echo "✅ Docker is running"
echo ""

# Navigate to ML directory
cd "$(dirname "$0")"

# Build and start services
echo "📦 Building ML API image..."
docker compose -f docker-compose-ml-api.yaml build ml-api

echo ""
echo "🎬 Starting services..."
docker compose -f docker-compose-ml-api.yaml up -d

echo ""
echo "⏳ Waiting for services to be healthy..."
sleep 10

# Check health
echo ""
echo "🏥 Checking service health..."
if curl -f http://localhost:8001/health > /dev/null 2>&1; then
    echo "✅ ML API is healthy!"
else
    echo "⚠️  ML API health check failed. Checking logs..."
    docker logs vinatien-ml-api --tail 50
    exit 1
fi

echo ""
echo "=================================="
echo "✅ VinaTien ML API is ready!"
echo "=================================="
echo ""
echo "📍 Service URLs:"
echo "   - API:          http://localhost:8001"
echo "   - Health:       http://localhost:8001/health"
echo "   - Docs (Swagger): http://localhost:8001/docs"
echo "   - ReDoc:        http://localhost:8001/redoc"
echo ""
echo "🧪 Test eligibility endpoint:"
echo '   curl -X POST http://localhost:8001/api/v1/ewa/eligibility \'
echo '     -H "Content-Type: application/json" \'
echo '     -d @api/tests/sample_request.json'
echo ""
echo "📊 View logs:"
echo "   docker logs vinatien-ml-api -f"
echo ""
echo "🛑 Stop services:"
echo "   docker compose -f docker-compose-ml-api.yaml down"
echo ""
