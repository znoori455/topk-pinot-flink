// src/main/kotlin/com/restaurant/topk/service/QueryService.kt
package com.restaurant.topk.service

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.restaurant.topk.models.TopKResult
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import java.util.concurrent.TimeUnit

/**
 * Service for querying Apache Pinot
 */
class QueryService(
    private val pinotBrokerUrl: String = "http://localhost:8099"

//    private val pinotBrokerUrl: String = "http://pinot-broker:8099"
) {

    private val client = OkHttpClient.Builder()
        .connectTimeout(5, TimeUnit.SECONDS)
        .readTimeout(10, TimeUnit.SECONDS)
        .build()

    private val objectMapper = jacksonObjectMapper().apply {
        // Ignore any unknown properties in the JSON
        configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    }
    private val jsonMediaType = "application/json".toMediaType()

    /**
     * Get Top K menu items for a single restaurant
     */
    fun getTopKForRestaurant(
        restaurantId: String,
        startTime: Long,
        endTime: Long,
        k: Int = 10
    ): List<TopKResult> {
        val sql = """
            SELECT 
                restaurant_id,
                menu_item_id,
                menu_item_name,
                rank,
                order_count,
                total_quantity,
                total_revenue_in_cents,
                window_start,
                window_end
            FROM restaurant_topk
            WHERE restaurant_id = '$restaurantId'
                AND window_start >= $startTime
                AND window_end <= $endTime
                AND rank <= $k
            ORDER BY window_end DESC, rank ASC
            LIMIT $k
        """.trimIndent()

        return executePinotQuery(sql)
    }

    /**
     * Get Top K menu items across all restaurants
     */
    fun getTopKForAllRestaurants(
        startTime: Long,
        endTime: Long,
        k: Int = 10
    ): List<TopKResult> {
        val sql = """
            SELECT 
                restaurant_id,
                menu_item_id,
                menu_item_name,
                rank,
                order_count,
                total_quantity,
                total_revenue_in_cents,
                window_start,
                window_end
            FROM global_topk
            WHERE window_start >= $startTime
                AND window_end <= $endTime
                AND rank <= $k
            ORDER BY window_end DESC, rank ASC
            LIMIT $k
        """.trimIndent()

        return executePinotQuery(sql)
    }

    /**
     * Get Top K with revenue ranking (alternative metric)
     */
    fun getTopKByRevenue(
        restaurantId: String?,
        startTime: Long,
        endTime: Long,
        k: Int = 10
    ): List<TopKResult> {
        val table = if (restaurantId != null) "restaurant_topk" else "global_topk"
        val restaurantFilter = if (restaurantId != null)
            "AND restaurant_id = '$restaurantId'" else ""

        val sql = """
            SELECT 
                restaurant_id,
                menu_item_id,
                menu_item_name,
                rank,
                order_count,
                total_quantity,
                total_revenue_in_cents,
                window_start,
                window_end
            FROM $table
            WHERE window_start >= $startTime
                AND window_end <= $endTime
                $restaurantFilter
            ORDER BY total_revenue_in_cents DESC, window_end DESC
            LIMIT $k
        """.trimIndent()

        return executePinotQuery(sql)
    }

    private fun executePinotQuery(sql: String): List<TopKResult> {
        val requestBody = mapOf("sql" to sql)
        val request = Request.Builder()
            .url("$pinotBrokerUrl/query/sql")
            .post(objectMapper.writeValueAsString(requestBody).toRequestBody(jsonMediaType))
            .build()

        client.newCall(request).execute().use { response ->
            if (!response.isSuccessful) {
                throw RuntimeException("Pinot query failed: ${response.code}")
            }

            val responseBody = response.body?.string()
                ?: throw RuntimeException("Empty response from Pinot")

            val result = objectMapper.readValue<PinotQueryResponse>(responseBody)
            return parseResults(result)
        }
    }

    private fun parseResults(response: PinotQueryResponse): List<TopKResult> {
        return response.resultTable.rows.map { row ->
            TopKResult(
                restaurantId = row[0] as String,
                menuItemId = row[1] as String,
                menuItemName = row[2] as String,
                rank = (row[3] as Number).toInt(),
                orderCount = (row[4] as Number).toLong(),
                totalQuantity = (row[5] as Number).toLong(),
                totalRevenueInCents = (row[6] as Number).toLong(),
                windowStart = (row[7] as Number).toLong(),
                windowEnd = (row[8] as Number).toLong()
            )
        }
    }
}

data class PinotQueryResponse(
    val resultTable: ResultTable
)

data class ResultTable(
    val dataSchema: DataSchema,
    val rows: List<List<Any>>
)

data class DataSchema(
    val columnNames: List<String>,
    val columnDataTypes: List<String>
)