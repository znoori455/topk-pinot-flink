# Restaurant Top K Real-Time Analytics System

A high-performance, near real-time analytics system for tracking Top K menu items across restaurants using Apache Flink, Apache Pinot, and Kotlin.

## 🎯 Features

- **Near Real-Time Processing**: Sub-second latency from order to query
- **High Throughput**: Supports 10-50 million orders per day
- **Duplicate Prevention**: Built-in deduplication mechanism
- **Flexible Queries**:
    - Top K menu items per restaurant
    - Top K menu items across all restaurants
    - Time-range filtering
- **Scalable Architecture**: Horizontal scaling with Flink and Pinot
- **Financial Accuracy**: Uses integer cents instead of floating-point for currency

## 🏗️ Architecture

```
Order Events → Kafka → Flink (Dedup & Aggregate) → Kafka → Pinot → Query API
                           ↓
                    Stateful Processing
                    (1-minute windows)
```

### Components

1. **Apache Kafka**: Message broker for order events and aggregated metrics
2. **Apache Flink**: Stream processing engine for:
    - Order deduplication (1-hour sliding window)
    - Real-time aggregation (1-minute tumbling windows)
    - Top K calculation using min-heap algorithm
3. **Apache Pinot**: OLAP database for low-latency queries
4. **Query API**: REST API built with Ktor for accessing Top K data
5. **Data Generator**: Testing tool that simulates realistic order volumes

## 📊 Data Model

### Order Event
```json
{
  "order_id": "order_123456",
  "restaurant_id": "restaurant_1",
  "menu_item_id": "margherita_pizza",
  "menu_item_name": "Margherita Pizza",
  "quantity": 2,
  "price_in_cents": 1299,
  "timestamp": 1698345600000
}
```

### Top K Result
```json
{
  "restaurant_id": "restaurant_1",
  "menu_item_id": "margherita_pizza",
  "menu_item_name": "Margherita Pizza",
  "rank": 1,
  "order_count": 1523,
  "total_quantity": 2891,
  "total_revenue_in_cents": 3753509,
  "window_start": 1698345600000,
  "window_end": 1698345660000
}
```

## 🚀 Getting Started

### Prerequisites

- Docker and Docker Compose
- Java 11 or higher
- Gradle 7.x or higher
- At least 8GB RAM available for Docker

### Quick Start


1. **Clone the repository**
```bash
 docker-compose -f docker-pinot.yml up
```

```bash
./gradlew clean shadowJar shadowJarApi shadowJarGenerator
mkdir -p target
cp build/libs/restaurant-topk.jar target/
cp build/libs/restaurant-topk-api.jar target/
cp build/libs/restaurant-topk-generator.jar target/
```

```bash
docker-compose up -d jobmanager taskmanager
docker exec jobmanager flink run \
  -d \
  -c com.restaurant.topk.flink.TopKStreamingJobKt \
  /opt/flink/usrlib/restaurant-topk.jar
```

```bash
docker-compose up -d data-generator
```


1. **Clone the repository**
```bash
git clone <repository-url>
cd restaurant-topk-system
```

2. **Make the setup script executable**
```bash
chmod +x setup.sh
```

3. **Run the setup script**
```bash
./setup.sh
```

This script will:
- Build all Kotlin applications
- Start all infrastructure components
- Create Kafka topics
- Set up Pinot tables
- Deploy the Flink job
- Start the Query API
- Start the data generator

### Manual Setup (Alternative)

If you prefer manual control:

```bash
# 1. Build the applications
./gradlew clean shadowJar shadowJarApi shadowJarGenerator
mkdir -p target
cp build/libs/*.jar target/

# 2. Start infrastructure
docker-compose up -d zookeeper kafka pinot-controller pinot-broker pinot-server

# 3. Wait for services (30 seconds)
sleep 30

# 4. Create Kafka topics
docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 \
  --replication-factor 1 --partitions 10 --topic restaurant-orders

docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 \
  --replication-factor 1 --partitions 10 --topic restaurant_topk

docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 \
  --replication-factor 1 --partitions 10 --topic global_topk

# 5. Create Pinot schemas and tables
curl -X POST "http://localhost:9000/schemas" \
  -H "Content-Type: application/json" \
  -d @config/restaurant_topk_schema.json

curl -X POST "http://localhost:9000/tables" \
  -H "Content-Type: application/json" \
  -d @config/restaurant_topk_table.json

curl -X POST "http://localhost:9000/schemas" \
  -H "Content-Type: application/json" \
  -d @config/global_topk_schema.json

curl -X POST "http://localhost:9000/tables" \
  -H "Content-Type: application/json" \
  -d @config/global_topk_table.json

# 6. Start Flink
docker-compose up -d flink-jobmanager flink-taskmanager
sleep 15

# 7. Submit Flink job
docker exec flink-jobmanager flink run -d \
  -c com.restaurant.topk.flink.TopKStreamingJob.kt \
  /opt/flink/usrlib/restaurant-topk.jar

# 8. Start Query API and Data Generator
docker-compose up -d query-api data-generator
```

## 📡 API Endpoints

### 1. Get Top K for a Single Restaurant

```bash
GET /api/v1/restaurants/{restaurantId}/topk

# Parameters:
# - start_time: Start timestamp in milliseconds (optional, defaults to 1 hour ago)
# - end_time: End timestamp in milliseconds (optional, defaults to now)
# - k: Number of top items (optional, defaults to 10)

# Example:
curl 'http://localhost:8080/api/v1/restaurants/restaurant_1/topk?k=5'
```

**Response:**
```json
{
  "restaurantId": "restaurant_1",
  "startTime": 1698342000000,
  "endTime": 1698345600000,
  "k": 5,
  "items": [
    {
      "restaurant_id": "restaurant_1",
      "menu_item_id": "margherita_pizza",
      "menu_item_name": "Margherita Pizza",
      "rank": 1,
      "order_count": 1523,
      "total_quantity": 2891,
      "total_revenue_in_cents": 3753509,
      "window_start": 1698345600000,
      "window_end": 1698345660000
    }
  ]
}
```

### 2. Get Top K Across All Restaurants

```bash
GET /api/v1/restaurants/all/topk

# Parameters: Same as above

# Example:
curl 'http://localhost:8080/api/v1/restaurants/all/topk?k=10'
```

### 3. Get Top K by Revenue

```bash
GET /api/v1/restaurants/{restaurantId}/topk/revenue

# Use "all" for all restaurants
# Parameters: Same as above

# Example:
curl 'http://localhost:8080/api/v1/restaurants/restaurant_1/topk/revenue?k=10'
curl 'http://localhost:8080/api/v1/restaurants/all/topk/revenue?k=10'
```

### 4. Health Check

```bash
GET /health

curl http://localhost:8080/health
```

## 🔍 Monitoring

### Flink Dashboard
Access the Flink web UI at: http://localhost:8081

- View running jobs
- Monitor checkpoint status
- Check task metrics
- View job topology

### Pinot Console
Access the Pinot console at: http://localhost:9000

- Query tables directly
- Monitor ingestion status
- View segment statistics
- Check cluster health

### View Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f flink-jobmanager
docker-compose logs -f query-api
docker-compose logs -f data-generator
docker-compose logs -f pinot-broker
```

## 🔧 Configuration

### Adjust Data Generation Rate

Edit `docker-compose.yml`:
```yaml
data-generator:
  environment:
    ORDERS_PER_SECOND: 500  # Change this value
```

For 50 million orders/day: `ORDERS_PER_SECOND: 579`

### Tune Flink Performance

Edit Flink task manager settings in `docker-compose.yml`:
```yaml
flink-taskmanager:
  scale: 4  # Increase number of task managers
  environment:
    - |
      FLINK_PROPERTIES=
      taskmanager.numberOfTaskSlots: 8  # Increase slots per task manager
      taskmanager.memory.process.size: 4096m  # Increase memory
```

### Pinot Optimization

The system is configured with:
- **Sorted columns**: `window_end` for time-based queries
- **Inverted indexes**: `restaurant_id`, `rank` for fast lookups
- **Range indexes**: `window_start`, `window_end` for time-range queries
- **Bloom filters**: `restaurant_id`, `menu_item_id` for existence checks

## 🎯 Performance Characteristics

### Throughput
- **Current configuration**: 100 orders/second (8.64M/day)
- **Tested up to**: 579 orders/second (50M/day)
- **Scalability**: Horizontal scaling via Flink task managers

### Latency
- **End-to-end latency**: < 2 seconds (order → queryable)
- **Query latency**: 10-100ms (P99)
- **Window size**: 1 minute (configurable)

### Data Retention
- **Pinot retention**: 7 days (configurable)
- **Flink state**: 1 hour for deduplication

## 🛡️ Reliability Features

### Deduplication
- Uses Flink keyed state with 1-hour TTL
- Prevents duplicate orders from being counted
- Handles replay scenarios gracefully

### Checkpointing
- Flink checkpoints every 60 seconds
- Enables exactly-once processing semantics
- Automatic recovery on failure

### High Availability
- Kafka replication (configurable)
- Pinot replication (configurable)
- Multiple Flink task managers

## 📝 Project Structure

```
restaurant-topk-system/
├── src/main/kotlin/com/restaurant/topk/
│   ├── models/              # Data models
│   ├── flink/               # Flink streaming job
│   ├── service/             # Query service
│   ├── api/                 # REST API
│   └── generator/           # Data generator
├── config/                  # Pinot schemas and tables
├── build.gradle.kts         # Build configuration
├── docker-compose.yml       # Infrastructure setup
├── setup.sh                 # Automated setup script
└── README.md               # This file
```

## 🧪 Testing

### Generate Test Orders

```bash
# Manually send a test order
docker exec kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic restaurant-orders << EOF
{"order_id":"test_001","restaurant_id":"restaurant_1","menu_item_id":"test_pizza","menu_item_name":"Test Pizza","quantity":1,"price_in_cents":1500,"timestamp":$(date +%s)000}
EOF
```

### Query Pinot Directly

```bash
curl -X POST "http://localhost:8099/query/sql" \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT * FROM restaurant_topk WHERE restaurant_id = '\''restaurant_1'\'' LIMIT 10"
  }'
```

### Check Kafka Topics

```bash
# List topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Consume from topic
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic restaurant_topk \
  --from-beginning --max-messages 10
```

## 🛑 Shutdown

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (clean slate)
docker-compose down -v
```

## 🔄 Restart

```bash
# Restart specific service
docker-compose restart query-api

# Restart all services
docker-compose restart
```

## 💡 Best Practices

1. **Monitor Kafka lag**: Ensure Flink keeps up with incoming data
2. **Tune window sizes**: Balance between latency and accuracy
3. **Set appropriate retention**: Based on query patterns
4. **Scale horizontally**: Add more Flink task managers for higher throughput
5. **Use proper timestamps**: Ensure event-time processing accuracy

## 🐛 Troubleshooting

### Flink job fails to start
- Check Kafka connectivity: `docker-compose logs kafka`
- Verify topic creation: `docker exec kafka kafka-topics --list --bootstrap-server localhost:9092`
- Check Flink logs: `docker-compose logs flink-jobmanager`

### No data in Pinot
- Verify Flink job is running: http://localhost:8081
- Check Kafka topics have data: Use kafka-console-consumer
- Review Pinot ingestion status: http://localhost:9000

### Query API returns empty results
- Ensure data generator is running: `docker-compose logs data-generator`
- Wait 2-3 minutes for data to flow through pipeline
- Check Pinot directly with SQL queries

### High memory usage
- Reduce `ORDERS_PER_SECOND` in data generator
- Increase Docker memory allocation
- Tune Flink memory settings

## 📚 Additional Resources

- [Apache Flink Documentation](https://flink.apache.org/docs/)
- [Apache Pinot Documentation](https://docs.pinot.apache.org/)
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Ktor Documentation](https://ktor.io/docs/)

## 📄 License

This project is provided as-is for educational and commercial use.

## 🤝 Contributing

Contributions are welcome! Please ensure all code follows Kotlin best practices and includes appropriate tests.