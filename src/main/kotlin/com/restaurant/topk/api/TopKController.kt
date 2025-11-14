// src/main/kotlin/com/restaurant/topk/api/TopKController.kt
package com.restaurant.topk.api

import com.restaurant.topk.models.TopKResult
import com.restaurant.topk.service.QueryService
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.serialization.jackson.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import java.time.Instant
import java.time.temporal.ChronoUnit

class TopKController(private val queryService: QueryService) {

    fun Application.module() {
        install(ContentNegotiation) {
            jackson()
        }

        routing {
            get("/health") {
                call.respond(mapOf("status" to "healthy"))
            }

            // Get Top K for a single restaurant
            get("/api/v1/restaurants/{restaurantId}/topk") {
                val restaurantId = call.parameters["restaurantId"]
                    ?: return@get call.respond(HttpStatusCode.BadRequest, "Missing restaurantId")

                val startTime = call.parameters["start_time"]?.toLongOrNull()
                    ?: Instant.now().minus(1, ChronoUnit.HOURS).toEpochMilli()
                val endTime = call.parameters["end_time"]?.toLongOrNull()
                    ?: Instant.now().toEpochMilli()
                val k = call.parameters["k"]?.toIntOrNull() ?: 10

                try {
                    val results = queryService.getTopKForRestaurant(
                        restaurantId = restaurantId,
                        startTime = startTime,
                        endTime = endTime,
                        k = k
                    )

                    call.respond(
                        TopKResponse(
                            restaurantId = restaurantId,
                            startTime = startTime,
                            endTime = endTime,
                            k = k,
                            items = results
                        )
                    )
                } catch (e: Exception) {
                    call.respond(
                        HttpStatusCode.InternalServerError,
                        mapOf("error" to e.message)
                    )
                }
            }

            // Get Top K across all restaurants
            get("/api/v1/restaurants/all/topk") {
                val startTime = call.parameters["start_time"]?.toLongOrNull()
                    ?: Instant.now().minus(1, ChronoUnit.HOURS).toEpochMilli()
                val endTime = call.parameters["end_time"]?.toLongOrNull()
                    ?: Instant.now().toEpochMilli()
                val k = call.parameters["k"]?.toIntOrNull() ?: 10

                try {
                    val results = queryService.getTopKForAllRestaurants(
                        startTime = startTime,
                        endTime = endTime,
                        k = k
                    )

                    call.respond(
                        TopKResponse(
                            restaurantId = "ALL",
                            startTime = startTime,
                            endTime = endTime,
                            k = k,
                            items = results
                        )
                    )
                } catch (e: Exception) {
                    call.respond(
                        HttpStatusCode.InternalServerError,
                        mapOf("error" to e.message)
                    )
                }
            }

            // Get Top K by revenue
            get("/api/v1/restaurants/{restaurantId}/topk/revenue") {
                val restaurantId = call.parameters["restaurantId"]
                val startTime = call.parameters["start_time"]?.toLongOrNull()
                    ?: Instant.now().minus(1, ChronoUnit.HOURS).toEpochMilli()
                val endTime = call.parameters["end_time"]?.toLongOrNull()
                    ?: Instant.now().toEpochMilli()
                val k = call.parameters["k"]?.toIntOrNull() ?: 10

                try {
                    val results = queryService.getTopKByRevenue(
                        restaurantId = if (restaurantId == "all") null else restaurantId,
                        startTime = startTime,
                        endTime = endTime,
                        k = k
                    )

                    call.respond(
                        TopKResponse(
                            restaurantId = restaurantId ?: "ALL",
                            startTime = startTime,
                            endTime = endTime,
                            k = k,
                            items = results
                        )
                    )
                } catch (e: Exception) {
                    call.respond(
                        HttpStatusCode.InternalServerError,
                        mapOf("error" to e.message)
                    )
                }
            }
        }
    }
}

data class TopKResponse(
    val restaurantId: String,
    val startTime: Long,
    val endTime: Long,
    val k: Int,
    val items: List<TopKResult>
)

fun main() {
    val queryService = QueryService()

    embeddedServer(Netty, port = 8080) {
        TopKController(queryService).apply {
            module()
        }
    }.start(wait = true)
}