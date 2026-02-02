#!/bin/bash
###############################################################################
# Emergency Cleanup: Remove Duplicate Transactions from ClickHouse
# 
# This script removes duplicate transaction records where the same transaction
# ID appears multiple times. Keeps the most recent record based on etl_loaded_at.
#
# Usage: 
#   chmod +x cleanup_transaction_duplicates.sh
#   ./cleanup_transaction_duplicates.sh
#
# Author: VinaTien ML Team
# Created: 2026-01-31
###############################################################################

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}╔════════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║  Emergency Cleanup: Duplicate Transactions in ClickHouse          ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Get ClickHouse container name
CLICKHOUSE_CONTAINER=$(docker ps --filter "name=clickhouse" --format "{{.Names}}" | head -n 1)

if [ -z "$CLICKHOUSE_CONTAINER" ]; then
    echo -e "${RED}❌ Error: ClickHouse container not found${NC}"
    echo "   Please make sure ClickHouse is running"
    exit 1
fi

echo -e "${GREEN}Found ClickHouse container: $CLICKHOUSE_CONTAINER${NC}"
echo ""

# Step 1: Check current state
echo -e "${YELLOW}📊 Step 1: Checking current state...${NC}"
echo "-----------------------------------------------------------------------"

STATS=$(docker exec $CLICKHOUSE_CONTAINER clickhouse-client --query "
SELECT
    COUNT(*) as total_rows,
    COUNT(DISTINCT id) as unique_ids,
    COUNT(*) - COUNT(DISTINCT id) as duplicate_rows
FROM vinatien_analytics.transactions_fact
FORMAT TabSeparated
")

TOTAL_ROWS=$(echo "$STATS" | cut -f1)
UNIQUE_IDS=$(echo "$STATS" | cut -f2)
DUPLICATE_ROWS=$(echo "$STATS" | cut -f3)

echo "Total rows in table: $TOTAL_ROWS"
echo "Unique transaction IDs: $UNIQUE_IDS"
echo "Duplicate rows: $DUPLICATE_ROWS"
echo ""

if [ "$DUPLICATE_ROWS" -eq 0 ]; then
    echo -e "${GREEN}✅ No duplicates found! Database is clean.${NC}"
    exit 0
fi

# Step 2: Show sample duplicates
echo -e "${YELLOW}📋 Step 2: Sample of duplicate transaction IDs...${NC}"
echo "-----------------------------------------------------------------------"

docker exec $CLICKHOUSE_CONTAINER clickhouse-client --query "
SELECT 
    id,
    COUNT(*) as duplicate_count,
    MIN(iban) as iban,
    MIN(etl_loaded_at) as first_loaded,
    MAX(etl_loaded_at) as last_loaded
FROM vinatien_analytics.transactions_fact
GROUP BY id
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
LIMIT 10
FORMAT PrettyCompact
"
echo ""

# Step 3: Confirmation
echo -e "${RED}⚠️  WARNING: About to delete $DUPLICATE_ROWS duplicate rows!${NC}"
echo -e "${YELLOW}Strategy: Keep the most recent record (MAX etl_loaded_at) for each ID${NC}"
echo ""
read -p "Do you want to proceed? (yes/no): " CONFIRMATION

if [ "$CONFIRMATION" != "yes" ]; then
    echo -e "${RED}❌ Cleanup cancelled.${NC}"
    exit 1
fi

# Step 4: Execute deletion
echo ""
echo -e "${YELLOW}🗑️  Step 3: Executing deletion...${NC}"
echo "-----------------------------------------------------------------------"

docker exec $CLICKHOUSE_CONTAINER clickhouse-client --query "
ALTER TABLE vinatien_analytics.transactions_fact
DELETE WHERE (id, etl_loaded_at) IN (
    SELECT id, etl_loaded_at
    FROM vinatien_analytics.transactions_fact
    QUALIFY row_number() OVER (
        PARTITION BY id 
        ORDER BY etl_loaded_at DESC
    ) > 1
)
"

echo -e "${GREEN}✅ Deletion query executed successfully!${NC}"
echo ""

# Step 5: Wait for ClickHouse to process
echo -e "${YELLOW}⏳ Waiting for ClickHouse to process deletion (5 seconds)...${NC}"
sleep 5

# Step 6: Verify cleanup
echo ""
echo -e "${YELLOW}🔍 Step 4: Verifying cleanup...${NC}"
echo "-----------------------------------------------------------------------"

NEW_STATS=$(docker exec $CLICKHOUSE_CONTAINER clickhouse-client --query "
SELECT
    COUNT(*) as total_rows,
    COUNT(DISTINCT id) as unique_ids,
    COUNT(*) - COUNT(DISTINCT id) as duplicate_rows
FROM vinatien_analytics.transactions_fact
FORMAT TabSeparated
")

NEW_TOTAL_ROWS=$(echo "$NEW_STATS" | cut -f1)
NEW_UNIQUE_IDS=$(echo "$NEW_STATS" | cut -f2)
NEW_DUPLICATE_ROWS=$(echo "$NEW_STATS" | cut -f3)

echo "Total rows after cleanup: $NEW_TOTAL_ROWS"
echo "Unique transaction IDs: $NEW_UNIQUE_IDS"
echo "Remaining duplicates: $NEW_DUPLICATE_ROWS"
echo ""

ROWS_DELETED=$((TOTAL_ROWS - NEW_TOTAL_ROWS))
echo -e "${GREEN}✅ Deleted $ROWS_DELETED rows${NC}"
echo ""

if [ "$NEW_DUPLICATE_ROWS" -eq 0 ]; then
    echo -e "${GREEN}╔════════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║                   ✅ SUCCESS! All duplicates removed                ║${NC}"
    echo -e "${GREEN}╚════════════════════════════════════════════════════════════════════╝${NC}"
else
    echo -e "${YELLOW}⚠️  WARNING: $NEW_DUPLICATE_ROWS duplicates still remain.${NC}"
    echo "   You may need to run this script again or investigate manually."
fi

echo ""
echo -e "${BLUE}📊 Final Statistics:${NC}"
docker exec $CLICKHOUSE_CONTAINER clickhouse-client --query "
SELECT
    COUNT(*) as total_rows,
    COUNT(DISTINCT id) as unique_ids,
    MIN(booking_date) as earliest_date,
    MAX(booking_date) as latest_date,
    COUNT(DISTINCT iban) as unique_ibans
FROM vinatien_analytics.transactions_fact
FORMAT PrettyCompact
"

echo ""
echo -e "${GREEN}🎉 Cleanup complete!${NC}"
