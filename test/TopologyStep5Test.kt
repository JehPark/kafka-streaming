import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TopologyTestDriver
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class TopologyStep5Test {
    @Test
    fun windowedAggregationCalculatesRunningAverage() {
        val topology = buildTradeTopology(
            inputTopic = "trades",
            filteredOutputTopic = "trades.filtered",
            avgOutputTopic = "trades.symbol.avg",
            userProfileTopic = "user-profiles",
            clickEventTopic = "click-events",
            enrichedClickOutputTopic = "click-events.enriched",
        )

        val props = buildStreamsProperties("step5-windowed-aggregation").apply {
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092")
            put(StreamsConfig.STATE_DIR_CONFIG, "build/test-state/step5-windowed-aggregation")
        }

        TopologyTestDriver(topology, props).use { driver ->
            val tradeInput = driver.createInputTopic(
                "trades",
                Serdes.String().serializer(),
                JsonSerializer<Trade>(),
            )

            val avgOutput = driver.createOutputTopic(
                "trades.symbol.avg",
                Serdes.String().deserializer(),
                JsonDeserializer<TradeStats>(TradeStats::class.java),
            )

            tradeInput.pipeInput("trade-1", Trade("t1", "aapl", "u1", 1L, 1500.0, 10L))
            tradeInput.pipeInput("trade-2", Trade("t2", "aapl", "u1", 5L, 750.0, 20L))

            val outputs = avgOutput.readValuesToList()

            assertTrue(outputs.isNotEmpty())
            assertEquals("AAPL", outputs.last().symbol)
            assertEquals(2, outputs.last().totalTrades)
            assertEquals(6L, outputs.last().totalQuantity)
            assertEquals(5250.0, outputs.last().totalAmount)
            assertEquals(875.0, outputs.last().averagePrice)
        }
    }

    @Test
    fun tuningConfigContainsStep5Defaults() {
        val props = buildStreamsProperties("step5-config")

        assertEquals("step5-config", props[StreamsConfig.APPLICATION_ID_CONFIG])
        assertEquals("localhost:9092", props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG])
        assertEquals("build/streams-state/step5-config", props[StreamsConfig.STATE_DIR_CONFIG])
        assertEquals(StreamsConfig.EXACTLY_ONCE_V2, props[StreamsConfig.PROCESSING_GUARANTEE_CONFIG])
        assertEquals("1", props[StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG].toString())
        assertEquals(
            "300000",
            defaultTradeStateLogConfig()[TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG],
        )
    }
}
