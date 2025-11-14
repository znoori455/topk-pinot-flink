// src/main/kotlin/com/restaurant/topk/kafka/KafkaConsumerTopKJob.kt
package com.restaurant.topk.flink

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.restaurant.topk.models.OrderEvent
import com.restaurant.topk.models.TopKResult
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

/**
 * Simplified Top K system using regular Kafka Consumer instead of Flink
 *
 * Features:
 * - In-memory deduplication with TTL
 * - Time-windowed aggregation (1-minute tumbling windows)
 * - Top K calculation per restaurant and globally
 * - Periodic output to Kafka topics
 *
 * Trade-offs vs Flink:
 * + Simpler to understand and debug
 * + No distributed coordination overhead
 * + Easier deployment (single process)
 * - No exactly-once semantics (at-least-once only)
 * - No automatic state recovery on crash
 * - Manual window management required
 * - Single-threaded processing (can be parallelized manually)
 */
class KafkaConsumerTopKJob(
    private val kafkaBootstrapServers: String = "localhost:9092",
    private val inputTopic: String = "restaurant-orders",
    private val outputTopicRestaurant: String = "restaurant_topk",
    private val outputTopicGlobal: String = "global_topk",
    private val k: Int = 10,
    private val windowSizeMs: Long = 60_000, // 1 minute
    private val deduplicationTtlMs: Long = 3_600_000 // 1 hour
) {

    private val logger = LoggerFactory.getLogger(KafkaConsumerTopKJob::class.java)
    private val objectMapper = jacksonObjectMapper()

    // Deduplication state: orderId -> timestamp when seen
    private val seenOrders = ConcurrentHashMap<String, Long>()

    // Current window state: "restaurantId:menuItemId" -> aggregated metrics
    private val currentWindowMetrics = ConcurrentHashMap<String, MenuItemMetrics>()

    // Window management
    private var currentWindowStart: Long = 0
    private var currentWindowEnd: Long = 0

    // Metrics
    private val processedOrders = AtomicLong(0)
    private val duplicateOrders = AtomicLong(0)

    private lateinit var consumer: KafkaConsumer<String, String>
    private lateinit var producer: KafkaProducer<String, String>
    private val scheduler = Executors.newScheduledThreadPool(2)

    @Volatile
    private var running = true

    fun start() {
        logger.info("Starting Kafka Consumer Top K Job")
        logger.info("Bootstrap servers: $kafkaBootstrapServers")
        logger.info("Input topic: $inputTopic")
        logger.info("Window size: ${windowSizeMs}ms")
        logger.info("K: $k")

        // Initialize Kafka consumer
        consumer = createConsumer()
        consumer.subscribe(listOf(inputTopic))

        // Initialize Kafka producer
        producer = createProducer()

        // Initialize window
        initializeWindow()

        // Schedule periodic tasks
        scheduleWindowFlush()
        scheduleDeduplicationCleanup()
        scheduleMetricsReport()

        // Main consumption loop
        try {
            while (running) {
                val records = consumer.poll(Duration.ofMillis(400))

                for (record in records) {
                    try {
                        processOrder(record.value())
                    } catch (e: Exception) {
                        logger.error("Error processing record: ${record.value()}", e)
                    }
                }
                flushWindow()
            }
        } catch (e: Exception) {
            logger.error("Error in main loop", e)
        } finally {
            shutdown()
        }
    }

    private fun createConsumer(): KafkaConsumer<String, String> {
        val props = Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers)
            put(ConsumerConfig.GROUP_ID_CONFIG, "topk-consumer-simple3")
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
//            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
//            put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
            put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500")
        }
        return KafkaConsumer(props)
    }

    private fun createProducer(): KafkaProducer<String, String> {
        val props = Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.ACKS_CONFIG, "1")
            put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
            put(ProducerConfig.LINGER_MS_CONFIG, "10")
        }
        return KafkaProducer(props)
    }

    private fun initializeWindow() {
        val now = System.currentTimeMillis()
        currentWindowStart = (now / windowSizeMs) * windowSizeMs
        currentWindowEnd = currentWindowStart + windowSizeMs
        logger.info("Initialized window: [$currentWindowStart, $currentWindowEnd)")
    }

    private fun processOrder(json: String) {
        val order = objectMapper.readValue<OrderEvent>(json)

        // Check if within current window
//        if (order.timestamp < currentWindowStart || order.timestamp >= currentWindowEnd) {
            // Order is outside current window, might be late or early
            // For simplicity, we'll skip orders outside the window
//            return
//        }

        // Deduplication
        if (isDuplicate(order.orderId)) {
            duplicateOrders.incrementAndGet()
            return
        }

        // Mark as seen
        seenOrders[order.orderId] = System.currentTimeMillis()

        // Aggregate
        val key = "${order.restaurantId}:${order.menuItemId}"
        currentWindowMetrics.compute(key) { _, existing ->
            if (existing == null) {
                MenuItemMetrics(
                    restaurantId = order.restaurantId,
                    menuItemId = order.menuItemId,
                    menuItemName = order.menuItemName,
                    orderCount = 1,
                    totalQuantity = order.quantity.toLong(),
                    totalRevenueInCents = order.priceInCents.toLong() * order.quantity
                )
            } else {
                existing.copy(
                    orderCount = existing.orderCount + 1,
                    totalQuantity = existing.totalQuantity + order.quantity,
                    totalRevenueInCents = existing.totalRevenueInCents + (order.priceInCents * order.quantity)
                )
            }
        }

        processedOrders.incrementAndGet()
    }

    private fun isDuplicate(orderId: String): Boolean {
        return seenOrders.containsKey(orderId)
    }

    private fun scheduleWindowFlush() {
        scheduler.scheduleAtFixedRate({
            try {
                flushWindow()
            } catch (e: Exception) {
                logger.error("Error flushing window", e)
            }
        }, 0, windowSizeMs, TimeUnit.MILLISECONDS)
    }

    private fun flushWindow() {
        logger.info("Flushing window [$currentWindowStart, $currentWindowEnd)")
        logger.info("Metrics count: ${currentWindowMetrics.size}")

        if (currentWindowMetrics.isEmpty()) {
            logger.info("No data in window, skipping")
            advanceWindow()
            return
        }

        // Calculate Top K per restaurant
        val perRestaurantTopK = calculateTopKPerRestaurant()

        // Calculate Top K globally
        val globalTopK = calculateTopKGlobal()

        // Send to Kafka
        sendTopKResults(perRestaurantTopK, outputTopicRestaurant)
        sendTopKResults(globalTopK, outputTopicGlobal)

        logger.info("Sent ${perRestaurantTopK.size} restaurant Top K results")
        logger.info("Sent ${globalTopK.size} global Top K results")

        // Clear window and advance
        currentWindowMetrics.clear()
        advanceWindow()
    }

    private fun advanceWindow() {
        currentWindowStart = currentWindowEnd
        currentWindowEnd = currentWindowStart + windowSizeMs
        logger.info("Advanced to new window: [$currentWindowStart, $currentWindowEnd)")
    }

    private fun calculateTopKPerRestaurant(): List<TopKResult> {
        val results = mutableListOf<TopKResult>()

        // Group by restaurant
        val byRestaurant = currentWindowMetrics.values.groupBy { it.restaurantId }

        // For each restaurant, get Top K
        byRestaurant.forEach { (restaurantId, metrics) ->
            val sorted = metrics.sortedByDescending { it.orderCount }
            val topK = sorted.take(k)

            topK.forEachIndexed { index, metric ->
                results.add(
                    TopKResult(
                        restaurantId = restaurantId,
                        menuItemId = metric.menuItemId,
                        menuItemName = metric.menuItemName,
                        rank = index + 1,
                        orderCount = metric.orderCount,
                        totalQuantity = metric.totalQuantity,
                        totalRevenueInCents = metric.totalRevenueInCents,
                        windowStart = currentWindowStart,
                        windowEnd = currentWindowEnd
                    )
                )
            }
        }

        return results
    }

    private fun calculateTopKGlobal(): List<TopKResult> {
        val sorted = currentWindowMetrics.values.sortedByDescending { it.orderCount }
        val topK = sorted.take(k)

        return topK.mapIndexed { index, metric ->
            TopKResult(
                restaurantId = "ALL",
                menuItemId = metric.menuItemId,
                menuItemName = metric.menuItemName,
                rank = index + 1,
                orderCount = metric.orderCount,
                totalQuantity = metric.totalQuantity,
                totalRevenueInCents = metric.totalRevenueInCents,
                windowStart = currentWindowStart,
                windowEnd = currentWindowEnd
            )
        }
    }

    private fun sendTopKResults(results: List<TopKResult>, topic: String) {
        results.forEach { result ->
            val json = objectMapper.writeValueAsString(result)
            val record = ProducerRecord(topic, result.menuItemId, json)
            producer.send(record) { metadata, exception ->
                if (exception != null) {
                    logger.error("Error sending to $topic", exception)
                }
            }
        }
    }

    private fun scheduleDeduplicationCleanup() {
        scheduler.scheduleAtFixedRate({
            try {
                cleanupDeduplicationState()
            } catch (e: Exception) {
                logger.error("Error cleaning deduplication state", e)
            }
        }, 60, 60, TimeUnit.SECONDS) // Run every minute
    }

    private fun cleanupDeduplicationState() {
        val now = System.currentTimeMillis()
        val cutoff = now - deduplicationTtlMs

        val toRemove = seenOrders.filterValues { timestamp -> timestamp < cutoff }.keys
        toRemove.forEach { seenOrders.remove(it) }

        if (toRemove.isNotEmpty()) {
            logger.debug("Cleaned up ${toRemove.size} old order IDs from deduplication cache")
        }
    }

    private fun scheduleMetricsReport() {
        scheduler.scheduleAtFixedRate({
            try {
                reportMetrics()
            } catch (e: Exception) {
                logger.error("Error reporting metrics", e)
            }
        }, 30, 30, TimeUnit.SECONDS)
    }

    private fun reportMetrics() {
        logger.info("=== Metrics Report ===")
        logger.info("Processed orders: ${processedOrders.get()}")
        logger.info("Duplicate orders: ${duplicateOrders.get()}")
        logger.info("Deduplication cache size: ${seenOrders.size}")
        logger.info("Current window metrics: ${currentWindowMetrics.size}")
        logger.info("=====================")
    }

    fun stop() {
        logger.info("Stopping Kafka Consumer Top K Job")
        running = false
    }

    private fun shutdown() {
        logger.info("Shutting down...")

        // Flush final window
        try {
            flushWindow()
        } catch (e: Exception) {
            logger.error("Error flushing final window", e)
        }

        // Close resources
        scheduler.shutdown()
        try {
            scheduler.awaitTermination(10, TimeUnit.SECONDS)
        } catch (e: InterruptedException) {
            logger.warn("Scheduler shutdown interrupted")
        }

        consumer.close()
        producer.close()

        logger.info("Shutdown complete")
    }
}

/**
 * Data class to hold aggregated metrics for a menu item
 */
data class MenuItemMetrics(
    val restaurantId: String,
    val menuItemId: String,
    val menuItemName: String,
    val orderCount: Long,
    val totalQuantity: Long,
    val totalRevenueInCents: Long
)

/**
 * Main entry point
 */
fun main(args: Array<String>) {
    val kafkaServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS") ?: "localhost:9093"
    val windowSizeMs = System.getenv("WINDOW_SIZE_MS")?.toLongOrNull() ?: 60_000

    val job = KafkaConsumerTopKJob(
        kafkaBootstrapServers = kafkaServers,
        windowSizeMs = windowSizeMs
    )

    // Shutdown hook
    Runtime.getRuntime().addShutdownHook(Thread {
        job.stop()
    })

    job.start()
}