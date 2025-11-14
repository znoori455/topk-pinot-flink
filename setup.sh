#!/bin/bash

set -e

echo "========================================="
echo "Restaurant Top K System Setup"
echo "========================================="

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Step 1: Build the application
echo -e "${GREEN}Step 1: Building application...${NC}"
./gradlew clean shadowJar shadowJarApi shadowJarGenerator

# Create target directory if it doesn't exist
mkdir -p target

# Copy JARs to target directory
cp build/libs/restaurant-topk.jar target/
cp build/libs/restaurant-topk-api.jar target/
cp build/libs/restaurant-topk-generator.jar target/

echo -e "${GREEN}✓ Application built successfully${NC}"

# Step 2: Start infrastructure
echo -e "${GREEN}Step 2: Starting infrastructure (Zookeeper, Kafka, Pinot)...${NC}"
docker-compose up -d zookeeper kafka pinot-controller pinot-broker pinot-server

echo "Waiting for Zookeeper to be ready..."
sleep 10

echo "Waiting for Kafka to be ready..."
max_retries=30
retry_count=0
until docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; do
  retry_count=$((retry_count + 1))
  if [ $retry_count -eq $max_retries ]; then
    echo "Kafka did not start in time"
    exit 1
  fi
  echo "Waiting for Kafka... ($retry_count/$max_retries)"
  sleep 5
done

echo -e "${GREEN}✓ Infrastructure started${NC}"

# Step 3: Create Kafka topics
echo -e "${GREEN}Step 3: Creating Kafka topics...${NC}"
docker exec kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 10 \
  --topic restaurant-orders \
  --if-not-exists

docker exec kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 10 \
  --topic restaurant_topk \
  --if-not-exists

docker exec kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 10 \
  --topic global_topk \
  --if-not-exists

echo -e "${GREEN}✓ Kafka topics created${NC}"

# Step 4: Create Pinot tables
echo -e "${GREEN}Step 4: Creating Pinot tables...${NC}"

# Wait for Pinot controller to be ready
max_retries=30
retry_count=0
until curl -s http://localhost:9000/health > /dev/null 2>&1; do
  retry_count=$((retry_count + 1))
  if [ $retry_count -eq $max_retries ]; then
    echo "Pinot controller did not start in time"
    exit 1
  fi
  echo "Waiting for Pinot controller... ($retry_count/$max_retries)"
  sleep 5
done

# Create restaurant_topk schema
curl -X POST "http://localhost:9000/schemas" \
  -H "Content-Type: application/json" \
  -d @config/restaurant_topk_schema.json

# Create restaurant_topk table
curl -X POST "http://localhost:9000/tables" \
  -H "Content-Type: application/json" \
  -d @config/restaurant_topk_table.json

# Create global_topk schema
curl -X POST "http://localhost:9000/schemas" \
  -H "Content-Type: application/json" \
  -d @config/global_topk_schema.json

# Create global_topk table
curl -X POST "http://localhost:9000/tables" \
  -H "Content-Type: application/json" \
  -d @config/global_topk_table.json

echo -e "${GREEN}✓ Pinot tables created${NC}"

# Step 5: Start Flink cluster
echo -e "${GREEN}Step 5: Starting Flink cluster...${NC}"
docker-compose up -d jobmanager taskmanager

sleep 15

echo -e "${GREEN}✓ Flink cluster started${NC}"

# Step 6: Submit Flink job
echo -e "${GREEN}Step 6: Submitting Flink streaming job...${NC}"
docker exec jobmanager flink run \
  -d \
  -c com.restaurant.topk.flink.TopKStreamingJobKt \
  /opt/flink/usrlib/restaurant-topk.jar

echo -e "${GREEN}✓ Flink job submitted${NC}"

# Step 7: Start Query API
echo -e "${GREEN}Step 7: Starting Query API...${NC}"
docker-compose up -d query-api

echo -e "${GREEN}✓ Query API started${NC}"

# Step 8: Start Data Generator
echo -e "${GREEN}Step 8: Starting Data Generator...${NC}"
docker-compose up -d data-generator

echo -e "${GREEN}✓ Data Generator started${NC}"

echo ""
echo "========================================="
echo -e "${GREEN}Setup Complete!${NC}"
echo "========================================="
echo ""
echo "Services:"
echo "  - Flink Dashboard: http://localhost:8081"
echo "  - Pinot Console: http://localhost:9000"
echo "  - Query API: http://localhost:8080"
echo ""
echo "Example API calls:"
echo "  # Top K for a specific restaurant"
echo "  curl 'http://localhost:8080/api/v1/restaurants/restaurant_1/topk?k=10'"
echo ""
echo "  # Top K across all restaurants"
echo "  curl 'http://localhost:8080/api/v1/restaurants/all/topk?k=10'"
echo ""
echo "  # Top K by revenue for a restaurant"
echo "  curl 'http://localhost:8080/api/v1/restaurants/restaurant_1/topk/revenue?k=10'"
echo ""
echo "To view logs:"
echo "  docker-compose logs -f [service-name]"
echo ""
echo "To stop all services:"
echo "  docker-compose down"
echo ""
echo -e "${YELLOW}Note: It may take a few minutes for data to flow through the pipeline${NC}"