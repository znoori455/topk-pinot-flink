//// src/main/kotlin/com/restaurant/topk/flink/TopKStreamingJob.kt
package com.restaurant.topk.flink



import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.restaurant.topk.models.TopKResult
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeHint
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.flink.connector.kafka.sink.KafkaSink
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.kafka.clients.producer.ProducerConfig
import java.io.Serializable
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime

// Data models
data class OrderEvent(
    @field:JsonProperty("event_id") var eventId: String,
    @field:JsonProperty("order_id") var orderId: String,
    @field:JsonProperty("customer_id") var customerId: String,
    @field:JsonProperty("restaurant_id") var restaurantId: String,
    @field:JsonProperty("menu_item_id") var menuItemId: String,
    @field:JsonProperty("category_id") var categoryId: String,
    @field:JsonProperty("menu_item_name") var menuItemName: String,
    @field:JsonProperty("quantity") var quantity: Int,
    @field:JsonProperty("price_in_cents") var priceInCents: Int,
    @field:JsonProperty("timestamp") var timestamp: Long
) : Serializable
//{
//    fun getRevenueCents(): Long = quantity.toLong() * priceInCents
//}

data class RollupEvent(
    @field:JsonProperty("restaurant_id") val restaurantId: String,
    @field:JsonProperty("menu_item_id") val menuItemId: String,
    @field:JsonProperty("menu_item_name") val menuItemName: String,
    @field:JsonProperty("category_id") val categoryId: String,
    @field:JsonProperty("window_start_1m") val windowStart1m: Long,
    @field:JsonProperty("window_start_ts") val windowStartTs: Long,
    @field:JsonProperty("hour_of_day") val hourOfDay: Int,
    @field:JsonProperty("day_of_week") val dayOfWeek: Int,
    @field:JsonProperty("sum_quantity") val sumQuantity: Long,
    @field:JsonProperty("sum_revenue_cents") val sumRevenueCents: Long,
    @field:JsonProperty("order_count") val orderCount: Long,
    @field:JsonProperty("unique_customers_hll") val uniqueCustomersHll: String
) : Serializable

// Aggregate accumulator for windowed aggregation
data class OrderAccumulator(
    var sumQuantity: Long = 0L,
    var sumRevenueCents: Long = 0L,
    var orderCount: Long = 0L,
    var uniqueCustomers: MutableSet<String> = mutableSetOf(),
    var restaurantId: String = "",
    var menuItemId: String = "",
    var menuItemName: String = "",
    var categoryId: String = ""
) : Serializable

// Deduplication function using Flink state
class DeduplicationFunction(private val ttlSeconds: Long = 3600) :
    KeyedProcessFunction<String, OrderEvent, OrderEvent>() {

    private var seenState: ValueState<Boolean>? = null

    fun open(parameters: Configuration) {
        val stateDescriptor = ValueStateDescriptor(
            "seen-events",
            TypeInformation.of(Boolean::class.java)
        )
        // Set TTL to avoid unbounded state growth
        val ttlConfig = org.apache.flink.api.common.state.StateTtlConfig
            .newBuilder(Duration.ofSeconds(ttlSeconds))
            .setUpdateType(org.apache.flink.api.common.state.StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(org.apache.flink.api.common.state.StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build()
        stateDescriptor.enableTimeToLive(ttlConfig)
        seenState = runtimeContext.getState(stateDescriptor)
    }

    override fun processElement(
        event: OrderEvent,
        ctx: Context,
        out: Collector<OrderEvent>
    ) {
        val seen = seenState?.value() ?: false
        if (!seen) {
            seenState?.update(true)
            out.collect(event)
        }
        // If seen, drop the duplicate
    }
}

// Aggregation function for window
class OrderAggregateFunction : AggregateFunction<OrderEvent, OrderAccumulator, OrderAccumulator> {

    override fun createAccumulator(): OrderAccumulator = OrderAccumulator()

    override fun add(event: OrderEvent, acc: OrderAccumulator): OrderAccumulator {
        acc.sumQuantity += event.quantity
        acc.sumRevenueCents += event.quantity.toLong() * event.priceInCents
        acc.orderCount += 1
        acc.uniqueCustomers.add(event.customerId)
        acc.restaurantId = event.restaurantId
        acc.menuItemId = event.menuItemId
        acc.menuItemName = event.menuItemName
        acc.categoryId = event.categoryId
        return acc
    }

    override fun getResult(acc: OrderAccumulator): OrderAccumulator = acc

    override fun merge(acc1: OrderAccumulator, acc2: OrderAccumulator): OrderAccumulator {
        acc1.sumQuantity += acc2.sumQuantity
        acc1.sumRevenueCents += acc2.sumRevenueCents
        acc1.orderCount += acc2.orderCount
        acc1.uniqueCustomers.addAll(acc2.uniqueCustomers)
        return acc1
    }
}

// Process window function to create final rollup event
class RollupProcessFunction :
    ProcessWindowFunction<OrderAccumulator, RollupEvent, String, TimeWindow>() {

    override fun process(
        key: String,
        context: Context,
        elements: Iterable<OrderAccumulator>,
        out: Collector<RollupEvent>
    ) {
        val acc = elements.first()
        val windowStart = context.window().start

        // Extract hour and day of week
        val zdt = ZonedDateTime.ofInstant(
            Instant.ofEpochMilli(windowStart),
            ZoneId.systemDefault()
        )

        // Simple HLL approximation - in production use actual HLL library
        val hllValue = "hll:${acc.uniqueCustomers.size}:${acc.uniqueCustomers.hashCode()}"

        val rollup = RollupEvent(
            restaurantId = acc.restaurantId,
            menuItemId = acc.menuItemId,
            menuItemName = acc.menuItemName,
            categoryId = acc.categoryId,
            windowStart1m = windowStart,
            windowStartTs = windowStart,
            hourOfDay = zdt.hour,
            dayOfWeek = zdt.dayOfWeek.value,
            sumQuantity = acc.sumQuantity,
            sumRevenueCents = acc.sumRevenueCents,
            orderCount = acc.orderCount,
            uniqueCustomersHll = hllValue
        )

        out.collect(rollup)
    }
}

// Main Flink job
class RestaurantOrderPipeline {

    companion object {
        private val mapper: ObjectMapper = jacksonObjectMapper()

        @JvmStatic
        fun main(args: Array<String>) {
            val pipeline = RestaurantOrderPipeline()
            pipeline.run(args)
        }
    }

    fun run(args: Array<String>) {
        // Parse arguments
        val kafkaBootstrap: String = System.getenv("KAFKA_BOOTSTRAP_SERVERS") ?: "localhost:9093"
        val inputTopic = args.getOrNull(1) ?: "restaurant-orders"
        val rawPinotTopic = args.getOrNull(2) ?: "restaurant-orders-raw"
        val rollupPinotTopic = args.getOrNull(3) ?: "restaurant-orders-1m-rollup"

        println("Starting Flink Pipeline:")
        println("  Kafka Bootstrap: $kafkaBootstrap")
        println("  Input Topic: $inputTopic")
        println("  Raw Pinot Topic: $rawPinotTopic")
        println("  Rollup Pinot Topic: $rollupPinotTopic")

        // Create execution environment
        val env = StreamExecutionEnvironment.getExecutionEnvironment()
        env.enableCheckpointing(60000) // Checkpoint every 60 seconds

        // Kafka source configuration
        val kafkaSource = KafkaSource.builder<String>()
            .setBootstrapServers(kafkaBootstrap)
            .setTopics(inputTopic)
            .setGroupId("flink-restaurant-orders-consumer")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(SimpleStringSchema())
            .build()

        // Read from Kafka
        val rawStream: DataStream<String> = env
            .fromSource(
                kafkaSource,
                WatermarkStrategy
                    .forBoundedOutOfOrderness<String>(Duration.ofSeconds(10))
                    .withTimestampAssigner { _, _ -> System.currentTimeMillis() },
                "Kafka Source"
            )

        // Parse JSON to OrderEvent
        val orderStream: DataStream<OrderEvent> = rawStream
            .map({ json -> mapper.readValue<OrderEvent>(json) },
                TypeInformation.of(object : TypeHint<OrderEvent>() {}))
            .name("Parse JSON")

        // Apply watermark strategy based on event timestamp
        val orderStreamWithWatermarks = orderStream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .forBoundedOutOfOrderness<OrderEvent>(Duration.ofSeconds(10))
                    .withTimestampAssigner { event, _ -> event.timestamp }
            )

        // Deduplication - key by event_id
        val dedupedStream = orderStreamWithWatermarks
            .keyBy { it.eventId }
            .process(DeduplicationFunction(), TypeInformation.of(object : TypeHint<OrderEvent>() {}))
            .name("Deduplication")

        // Pipeline 1: Write deduplicated raw events to Pinot (via Kafka)
        writeRawEventsToPinot(dedupedStream, rawPinotTopic, kafkaBootstrap)

        // Pipeline 2: Create 1-minute rollups and write to Pinot (via Kafka)
        createRollupsAndWriteToPinot(dedupedStream, rollupPinotTopic, kafkaBootstrap)

        // Execute the job
        env.execute("Restaurant Order Processing Pipeline")
    }




    private fun writeRawEventsToPinot(
        stream: DataStream<OrderEvent>,
        topic: String,
        kafkaBootstrap: String
    ) {
        val kafkaSink= KafkaSink.builder<String>()
            .setBootstrapServers(kafkaBootstrap)
            .setRecordSerializer(
                org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema.builder<String>()
                    .setTopic(topic)
                    .setValueSerializationSchema(SimpleStringSchema())
                    .build()
            )
//            .setRecordSerializer(
//                KafkaRecordSerializationSchema.builder<OrderEvent>()
//                    .setTopic(topic)
//                    .setValueSerializationSchema { event: OrderEvent ->
//                        mapper.writeValueAsBytes(event)
//                    }
//                    .build()
//            )
            .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "900000")
            .build()

        stream
            .map(OrderEventSerializer())
            .sinkTo(kafkaSink)
            .name("Write to Pinot Raw Table (via Kafka)")
    }

    private fun createRollupsAndWriteToPinot(
        stream: DataStream<OrderEvent>,
        topic: String,
        kafkaBootstrap: String
    ) {
        // Create composite key: restaurant_id + menu_item_id
//        val rollupStream = stream
//            .keyBy { "${it.restaurantId}:${it.menuItemId}" }
//            .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
//            .aggregate(
//                OrderAggregateFunction(),
//                RollupProcessFunction(),
//                TypeInformation.of(object : TypeHint<OrderAccumulator>() {}),
//                TypeInformation.of(object : TypeHint<RollupEvent>() {})
//            )
//            .name("1-Minute Rollup Aggregation")
        val rollupStream = stream
            .keyBy { "${it.restaurantId}:${it.menuItemId}" }
            .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
            .aggregate(OrderAggregateFunction(), RollupProcessFunction())
            .name("1-Minute Rollup Aggregation")


        val kafkaSink= KafkaSink.builder<String>()
            .setBootstrapServers(kafkaBootstrap)
            .setRecordSerializer(
                org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema.builder<String>()
                    .setTopic(topic)
                    .setValueSerializationSchema(SimpleStringSchema())
                    .build()
            )
            .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "900000")
            .build()

//        val kafkaSink = KafkaSink.builder<String>()
//            .setBootstrapServers(kafkaBootstrap)
//            .setRecordSerializer(
//                org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema.builder<String>()
//                    .setTopic(topic)
//                    .setValueSerializationSchema(SimpleStringSchema())
//                    .build()
//            )
//            .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "900000")
//            .build()

        rollupStream
            .map(RollupEventSerializer())
            .sinkTo(kafkaSink)
            .name("Write to Pinot Rollup Table (via Kafka)")
    }
}

class OrderEventSerializer : MapFunction<OrderEvent, String> {
    @Transient
    private var objectMapper: ObjectMapper? = null

    private fun getMapper(): ObjectMapper {
        if (objectMapper == null) {
            objectMapper = jacksonObjectMapper()
        }
        return objectMapper!!
    }

    override fun map(value: OrderEvent): String {
        return getMapper().writeValueAsString(value)
    }
}

class RollupEventSerializer : MapFunction<RollupEvent, String> {
    @Transient
    private var objectMapper: ObjectMapper? = null

    private fun getMapper(): ObjectMapper {
        if (objectMapper == null) {
            objectMapper = jacksonObjectMapper()
        }
        return objectMapper!!
    }

    override fun map(value: RollupEvent): String {
        return getMapper().writeValueAsString(value)
    }
}

fun main(args: Array<String>) {
    RestaurantOrderPipeline.main(args)
}















//
//import com.restaurant.topk.models.MenuItemMetric
//import com.restaurant.topk.models.OrderEvent
//import com.restaurant.topk.models.TopKResult
//import org.apache.flink.api.common.eventtime.WatermarkStrategy
//import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner
//import org.apache.flink.api.common.functions.AggregateFunction
//import org.apache.flink.api.common.functions.MapFunction
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.connector.kafka.source.KafkaSource
//import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
//import org.apache.flink.streaming.api.datastream.DataStream
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
//import org.apache.flink.streaming.api.CheckpointingMode
//import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
//import org.apache.flink.util.Collector
//import com.fasterxml.jackson.databind.ObjectMapper
//import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
//import com.fasterxml.jackson.module.kotlin.readValue
//import java.time.Duration
//
//class TopKStreamingJob(
//    private val kafkaBootstrapServers: String = System.getenv("KAFKA_BOOTSTRAP_SERVERS") ?: "localhost:9093",
//    private val enableCheckpointing: Boolean = System.getenv("ENABLE_CHECKPOINTING")?.toBoolean() ?: true
//) {
//
//    private val K = 10 // Top K items
//
//    fun execute() {
//        val env = StreamExecutionEnvironment.getExecutionEnvironment()
//
//        // Configure checkpointing (optional for local debugging)
//        if (enableCheckpointing) {
//            env.enableCheckpointing(60000) // Checkpoint every minute
//            val checkpointConfig = env.checkpointConfig
//            checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
//            checkpointConfig.setMinPauseBetweenCheckpoints(30000)
//            checkpointConfig.setCheckpointTimeout(600000)
//            checkpointConfig.setMaxConcurrentCheckpoints(1)
//
//        }
//
//        // Configure Kafka source
//        val kafkaSource = KafkaSource.builder<String>()
//            .setBootstrapServers(kafkaBootstrapServers)
//            .setTopics("restaurant-orders")
//            .setGroupId("topk-consumer")
//            .setStartingOffsets(OffsetsInitializer.earliest())
//            .setValueOnlyDeserializer(SimpleStringSchema())
//            .build()
//
//        val orderStream = env
//            .fromSource(
//                kafkaSource,
//                WatermarkStrategy
//                    .forBoundedOutOfOrderness<String>(Duration.ofSeconds(10))
//                    .withTimestampAssigner(OrderTimestampAssigner()),
//                "Kafka Source"
//            )
//            .map(OrderDeserializationMapper())
//            .uid("parse-orders")
//
//        // Process orders with deduplication
//        val dedupedStream = deduplicateOrders(orderStream)
//
//        // Aggregate metrics per restaurant per menu item
//        val metricsStream = aggregateMetrics(dedupedStream)
//
//        // Calculate Top K per restaurant
//        val topKPerRestaurant = calculateTopKPerRestaurant(metricsStream)
//
//        // Calculate Top K across all restaurants
//        val topKAllRestaurants = calculateTopKAllRestaurants(metricsStream)
//
//        // Write to Pinot
//        writeToPinot(topKPerRestaurant, "restaurant_topk")
//        writeToPinot(topKAllRestaurants, "global_topk")
//
//        env.execute("Restaurant Top K Streaming Job")
//    }
//
//    private fun deduplicateOrders(stream: DataStream<OrderEvent>): DataStream<OrderEvent> {
//        return stream
//            .keyBy { it.orderId }
//            .process(DeduplicationProcessFunction(Duration.ofHours(1)))
//            .uid("deduplicate-orders")
//    }
//
//    private fun aggregateMetrics(stream: DataStream<OrderEvent>): DataStream<MenuItemMetric> {
//        return stream
//            .keyBy { "${it.restaurantId}:${it.menuItemId}" }
//            .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
//            .aggregate(MenuItemAggregator())
//            .uid("aggregate-metrics")
//    }
//
//    private fun calculateTopKPerRestaurant(stream: DataStream<MenuItemMetric>): DataStream<TopKResult> {
//        return stream
//            .keyBy { it.restaurantId }
//            .process(TopKProcessFunction(K))
//            .uid("topk-per-restaurant")
//    }
//
//    private fun calculateTopKAllRestaurants(stream: DataStream<MenuItemMetric>): DataStream<TopKResult> {
//        return stream
//            .keyBy { "global" }
//            .process(TopKProcessFunction(K, isGlobal = true))
//            .uid("topk-all-restaurants")
//    }
//
//    private fun writeToPinot(stream: DataStream<TopKResult>, tableName: String) {
//        // Convert to JSON and write to Kafka topic
//        stream
//            .map(TopKResultSerializer())
//            .uid("serialize-$tableName")
//            .sinkTo(
//                org.apache.flink.connector.kafka.sink.KafkaSink.builder<String>()
//                    .setBootstrapServers(kafkaBootstrapServers)
//                    .setRecordSerializer(
//                        org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema.builder<String>()
//                            .setTopic(tableName)
//                            .setValueSerializationSchema(SimpleStringSchema())
//                            .build()
//                    )
//                    .setDeliveryGuarantee(org.apache.flink.connector.base.DeliveryGuarantee.AT_LEAST_ONCE)
//                    .build()
//            )
//            .uid("sink-$tableName")
//    }
//}
//
///**
// * Aggregates order events into menu item metrics
// */
//class MenuItemAggregator : AggregateFunction<OrderEvent, MenuItemAccumulator, MenuItemMetric> {
//
//    override fun createAccumulator(): MenuItemAccumulator {
//        return MenuItemAccumulator()
//    }
//
//    override fun add(value: OrderEvent, acc: MenuItemAccumulator): MenuItemAccumulator {
//        acc.restaurantId = value.restaurantId
//        acc.menuItemId = value.menuItemId
//        acc.menuItemName = value.menuItemName
//        acc.orderCount += 1
//        acc.totalQuantity += value.quantity
//        acc.totalRevenueInCents += value.priceInCents * value.quantity
//        return acc
//    }
//
//    override fun getResult(acc: MenuItemAccumulator): MenuItemMetric {
//        return MenuItemMetric(
//            restaurantId = acc.restaurantId,
//            menuItemId = acc.menuItemId,
//            menuItemName = acc.menuItemName,
//            orderCount = acc.orderCount,
//            totalQuantity = acc.totalQuantity,
//            totalRevenueInCents = acc.totalRevenueInCents,
//            windowStart = acc.windowStart,
//            windowEnd = acc.windowEnd
//        )
//    }
//
//    override fun merge(a: MenuItemAccumulator, b: MenuItemAccumulator): MenuItemAccumulator {
//        a.orderCount += b.orderCount
//        a.totalQuantity += b.totalQuantity
//        a.totalRevenueInCents += b.totalRevenueInCents
//        return a
//    }
//}
//
//data class MenuItemAccumulator(
//    var restaurantId: String = "",
//    var menuItemId: String = "",
//    var menuItemName: String = "",
//    var orderCount: Long = 0,
//    var totalQuantity: Long = 0,
//    var totalRevenueInCents: Long = 0,
//    var windowStart: Long = 0,
//    var windowEnd: Long = 0
//)
//
///**
// * Serializable timestamp assigner for OrderEvent
// */
//class OrderTimestampAssigner : SerializableTimestampAssigner<String> {
//    @Transient
//    private var objectMapper: ObjectMapper? = null
//
//    private fun getMapper(): ObjectMapper {
//        if (objectMapper == null) {
//            objectMapper = jacksonObjectMapper()
//        }
//        return objectMapper!!
//    }
//
//    override fun extractTimestamp(element: String, recordTimestamp: Long): Long {
//        val order = getMapper().readValue<OrderEvent>(element)
//        return order.timestamp
//    }
//}
//
///**
// * Serializable mapper for deserializing OrderEvent from JSON
// */
//class OrderDeserializationMapper : MapFunction<String, OrderEvent> {
//    @Transient
//    private var objectMapper: ObjectMapper? = null
//
//    private fun getMapper(): ObjectMapper {
//        if (objectMapper == null) {
//            objectMapper = jacksonObjectMapper()
//        }
//        return objectMapper!!
//    }
//
//    override fun map(value: String): OrderEvent {
//        return getMapper().readValue(value)
//    }
//}
//
///**
// * Serializable mapper for serializing TopKResult to JSON
// */
//class TopKResultSerializer : MapFunction<TopKResult, String> {
//    @Transient
//    private var objectMapper: ObjectMapper? = null
//
//    private fun getMapper(): ObjectMapper {
//        if (objectMapper == null) {
//            objectMapper = jacksonObjectMapper()
//        }
//        return objectMapper!!
//    }
//
//    override fun map(value: TopKResult): String {
//        return getMapper().writeValueAsString(value)
//    }
//}
//
//fun main() {
//    TopKStreamingJob().execute()
//}
//
