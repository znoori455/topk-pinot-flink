package com.restaurant.topk.generator

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.PropertyNamingStrategies
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.io.Serializable
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.random.Random

data class OrderEvent(
    @field:JsonProperty("event_id") val eventId: String,
    @field:JsonProperty("order_id") val orderId: String,
    @field:JsonProperty("customer_id") val customerId: String,
    @field:JsonProperty("restaurant_id") val restaurantId: String,
    @field:JsonProperty("menu_item_id") val menuItemId: String,
    @field:JsonProperty("category_id") val categoryId: String,
    @field:JsonProperty("menu_item_name") val menuItemName: String,
    @field:JsonProperty("quantity") val quantity: Int,
    @field:JsonProperty("price_in_cents") val priceInCents: Int,
    @field:JsonProperty("timestamp") val timestamp: Long
) : Serializable

data class MenuItem(
    val id: String,
    val name: String,
    val categoryId: String,
    val priceInCents: Int
)

data class RevenueEntry(
    val menuItemId: String,
    val menuItemName: String,
    var totalRevenue: Long
) : Comparable<RevenueEntry> {
    override fun compareTo(other: RevenueEntry): Int = totalRevenue.compareTo(other.totalRevenue)
}

class DataGenerator(
    private val kafkaBootstrapServers: String,
    private val topic: String
) {
    private val mapper = ObjectMapper()
    private val producer: KafkaProducer<String, String>
    private val running = AtomicBoolean(true)

    // Top 10 heap (min-heap to keep largest 10)
    private val revenueHeap = PriorityQueue<RevenueEntry>()
    private val revenueMap = mutableMapOf<String, RevenueEntry>()

    private val restaurants = listOf("REST001", "REST002", "REST003", "REST004", "REST005")
    private val categories = listOf("CAT001", "CAT002", "CAT003", "CAT004")

    private val menuItems = listOf(
        MenuItem("ITEM001", "Margherita Pizza", "CAT001", 1299),
        MenuItem("ITEM002", "Pepperoni Pizza", "CAT001", 1499),
        MenuItem("ITEM003", "Caesar Salad", "CAT002", 899),
        MenuItem("ITEM004", "Greek Salad", "CAT002", 999),
        MenuItem("ITEM005", "Spaghetti Carbonara", "CAT003", 1399),
        MenuItem("ITEM006", "Lasagna", "CAT003", 1599),
        MenuItem("ITEM007", "Tiramisu", "CAT004", 699),
        MenuItem("ITEM008", "Cheesecake", "CAT004", 799),
        MenuItem("ITEM009", "Burger Deluxe", "CAT001", 1199),
        MenuItem("ITEM010", "Fish Tacos", "CAT001", 1099),
        MenuItem("ITEM011", "Pad Thai", "CAT003", 1249),
        MenuItem("ITEM012", "Sushi Roll", "CAT003", 1899),
        MenuItem("ITEM013", "Ice Cream Sundae", "CAT004", 599),
        MenuItem("ITEM014", "Chicken Wings", "CAT001", 999),
        MenuItem("ITEM015", "Nachos Supreme", "CAT002", 1099)
    )

    init {
        val props = Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.ACKS_CONFIG, "all")
        }
        producer = KafkaProducer(props)

        // Initialize revenue map
        menuItems.forEach { item ->
            revenueMap[item.id] = RevenueEntry(item.id, item.name, 0L)
        }

        // Shutdown hook to print heap
        Runtime.getRuntime().addShutdownHook(Thread {
            println("\n=== SHUTDOWN: Top 10 Menu Items by Revenue ===")
            printTopItems()
            producer.close()
        })
    }

    private fun updateRevenue(menuItemId: String, revenue: Long) {
        val entry = revenueMap[menuItemId] ?: return

        // Remove from heap if present
        revenueHeap.remove(entry)

        // Update revenue
        entry.totalRevenue += revenue

        // Maintain top 10
        if (revenueHeap.size < 10) {
            revenueHeap.offer(entry)
        } else {
            val min = revenueHeap.peek()
            if (entry.totalRevenue > min.totalRevenue) {
                revenueHeap.poll()
                revenueHeap.offer(entry)
            }
        }
    }

    private fun printTopItems() {
        val sorted = revenueHeap.sortedByDescending { it.totalRevenue }
        println("\nRank | Menu Item ID | Menu Item Name          | Total Revenue")
        println("-----|--------------|-------------------------|---------------")
        sorted.forEachIndexed { idx, entry ->
            val revenue = "$%.2f".format(entry.totalRevenue / 100.0)
            println("${(idx + 1).toString().padStart(4)} | ${entry.menuItemId.padEnd(12)} | ${entry.menuItemName.padEnd(23)} | ${revenue.padStart(13)}")
        }
        println("\nTotal events processed: ${revenueMap.values.sumOf { it.totalRevenue / 100 }} dollars")
    }

    fun generateEvents(count: Int, delayMs: Long = 100) {
        println("Starting event generation (press Ctrl+C to stop and see results)...")

        repeat(count) { i ->
            if (!running.get()) return

            val menuItem = menuItems.random()
            val quantity = Random.nextInt(1, 5)
            val revenue = (menuItem.priceInCents * quantity).toLong()

            val event = OrderEvent(
                eventId = UUID.randomUUID().toString(),
                orderId = "ORD${UUID.randomUUID().toString().substring(0, 8)}",
                customerId = "CUST${Random.nextInt(1000, 9999)}",
                restaurantId = restaurants.random(),
                menuItemId = menuItem.id,
                categoryId = menuItem.categoryId,
                menuItemName = menuItem.name,
                quantity = quantity,
                priceInCents = menuItem.priceInCents,
                timestamp = System.currentTimeMillis()
            )

            // Update revenue tracking
            updateRevenue(menuItem.id, revenue)

            // Send to Kafka
            val json = mapper.writeValueAsString(event)
            producer.send(ProducerRecord(topic, event.orderId, json))

            if ((i + 1) % 100 == 0) {
                println("Generated ${i + 1} events...")
            }

            Thread.sleep(delayMs)
        }

        println("\nCompleted generating $count events!")
    }

    fun stop() {
        running.set(false)
    }
}

fun main(args: Array<String>) {
    val bootstrapServers =  System.getenv("KAFKA_BOOTSTRAP_SERVERS") ?: "localhost:9093"
    val topic = "restaurant-orders"
    val eventCount = System.getenv("ORDERS_TO_GENERATE")?.toIntOrNull() ?: 3
    val delayMs = args.getOrNull(3)?.toLongOrNull() ?: 10L

    println("Kafka Bootstrap Servers: $bootstrapServers")
    println("Topic: $topic")
    println("Event Count: $eventCount")
    println("Delay: ${delayMs}ms between events\n")

    val generator = DataGenerator(bootstrapServers, topic)
    generator.generateEvents(eventCount, delayMs)
}