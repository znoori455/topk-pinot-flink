// src/main/kotlin/com/restaurant/topk/flink/ProcessFunctions.kt
package com.restaurant.topk.flink

import com.restaurant.topk.models.MenuItemMetric
import com.restaurant.topk.models.OrderEvent
import com.restaurant.topk.models.TopKResult
import org.apache.flink.api.common.functions.OpenContext
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.state.MapState
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import java.time.Duration
import java.util.PriorityQueue

/**
 * Deduplicates orders based on orderId within a time window
 */
class DeduplicationProcessFunction(
    private val retentionTime: Duration
) : KeyedProcessFunction<String, OrderEvent, OrderEvent>() {

    private lateinit var seenState: ValueState<Boolean>

    override fun open(openContext: OpenContext) {
        seenState = runtimeContext.getState(
            ValueStateDescriptor("seen", Boolean::class.java)
        )
    }

    override fun processElement(
        value: OrderEvent,
        ctx: Context,
        out: Collector<OrderEvent>
    ) {
        if (seenState.value() == null) {
            seenState.update(true)
            out.collect(value)

            // Register timer to clean up state
            ctx.timerService().registerEventTimeTimer(
                value.timestamp + retentionTime.toMillis()
            )
        }
        // If already seen, drop the duplicate
    }

    override fun onTimer(
        timestamp: Long,
        ctx: OnTimerContext,
        out: Collector<OrderEvent>
    ) {
        seenState.clear()
    }
}

/**
 * Maintains Top K menu items using a min-heap
 */
class TopKProcessFunction(
    private val k: Int,
    private val isGlobal: Boolean = false
) : KeyedProcessFunction<String, MenuItemMetric, TopKResult>() {

    private lateinit var topKState: MapState<String, MenuItemMetric>

    override fun open(openContext: OpenContext) {
        topKState = runtimeContext.getMapState(
            MapStateDescriptor(
                "topk-state",
                String::class.java,
                MenuItemMetric::class.java
            )
        )
    }

    override fun processElement(
        value: MenuItemMetric,
        ctx: Context,
        out: Collector<TopKResult>
    ) {
        val key = if (isGlobal) {
            value.menuItemId
        } else {
            "${value.restaurantId}:${value.menuItemId}"
        }

        // Update or insert the metric
        val existing = topKState.get(key)
        if (existing == null || value.orderCount > existing.orderCount) {
            topKState.put(key, value)
        }

        // Calculate Top K
        val allMetrics = mutableListOf<MenuItemMetric>()
        topKState.entries().forEach { entry ->
            allMetrics.add(entry.value)
        }

        // Sort by order count descending
        allMetrics.sortByDescending { it.orderCount }

        // Keep only top K in state
        val topK = allMetrics.take(k)
        topKState.clear()
        topK.forEach { metric ->
            val stateKey = if (isGlobal) {
                metric.menuItemId
            } else {
                "${metric.restaurantId}:${metric.menuItemId}"
            }
            topKState.put(stateKey, metric)
        }

        // Emit Top K results
        topK.forEachIndexed { index, metric ->
            out.collect(
                TopKResult(
                    restaurantId = if (isGlobal) "ALL" else metric.restaurantId,
                    menuItemId = metric.menuItemId,
                    menuItemName = metric.menuItemName,
                    rank = index + 1,
                    orderCount = metric.orderCount,
                    totalQuantity = metric.totalQuantity,
                    totalRevenueInCents = metric.totalRevenueInCents,
                    windowStart = metric.windowStart,
                    windowEnd = metric.windowEnd
                )
            )
        }
    }
}