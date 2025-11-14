// src/main/kotlin/com/restaurant/topk/models/Models.kt
package com.restaurant.topk.models

import com.fasterxml.jackson.annotation.JsonProperty
import java.io.Serializable

/**
 * Order event from the source system
 */
data class OrderEvent(
    @JsonProperty("order_id") val orderId: String,
    @JsonProperty("customer_id") val customer_id: String?,
    @JsonProperty("restaurant_id") val restaurantId: String,
    @JsonProperty("menu_item_id") val menuItemId: String,
    @JsonProperty("category_id") val categoryId: String?,
    @JsonProperty("menu_item_name") val menuItemName: String,
    @JsonProperty("quantity") val quantity: Int,
    @JsonProperty("price_in_cents") val priceInCents: Int,
    @JsonProperty("timestamp") val timestamp: Long
) : Serializable

/**
 * Aggregated metric for menu items
 */
data class MenuItemMetric(
    @JsonProperty("restaurant_id") val restaurantId: String,
    @JsonProperty("menu_item_id") val menuItemId: String,
    @JsonProperty("menu_item_name") val menuItemName: String,
    @JsonProperty("order_count") val orderCount: Long,
    @JsonProperty("total_quantity") val totalQuantity: Long,
    @JsonProperty("total_revenue_in_cents") val totalRevenueInCents: Long,
    @JsonProperty("window_start") val windowStart: Long,
    @JsonProperty("window_end") val windowEnd: Long
) : Serializable

/**
 * Top K result for a single restaurant
 */
data class TopKResult(
    @JsonProperty("restaurant_id") val restaurantId: String,
    @JsonProperty("menu_item_id") val menuItemId: String,
    @JsonProperty("menu_item_name") val menuItemName: String,
    @JsonProperty("rank") val rank: Int,
    @JsonProperty("order_count") val orderCount: Long,
    @JsonProperty("total_quantity") val totalQuantity: Long,
    @JsonProperty("total_revenue_in_cents") val totalRevenueInCents: Long,
    @JsonProperty("window_start") val windowStart: Long,
    @JsonProperty("window_end") val windowEnd: Long
) : Serializable