import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TopologyTestDriver
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class TopologyStep4Test {
    private fun createTopologyTestDriver(applicationId: String): TopologyTestDriver {
        val topology = buildTradeTopology(
            inputTopic = "trades",
            filteredOutputTopic = "trades.filtered",
            avgOutputTopic = "trades.symbol.avg",
            userProfileTopic = "user-profiles",
            clickEventTopic = "click-events",
            enrichedClickOutputTopic = "click-events.enriched",
        )

        val props = java.util.Properties().apply {
            putAll(buildStreamsProperties(applicationId))
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092")
            put(StreamsConfig.STATE_DIR_CONFIG, "build/test-state/$applicationId")
        }

        return TopologyTestDriver(topology, props)
    }

    @Test
    fun filterAndMapTransformsTradeAndDropsSmallAmountTrades() {
        createTopologyTestDriver("step4-filter-map").use { driver ->

            val tradeInput = driver.createInputTopic(
                "trades",
                Serdes.String().serializer(),
                JsonSerializer<Trade>(),
            )
            val filteredOutput = driver.createOutputTopic(
                "trades.filtered",
                Serdes.String().deserializer(),
                JsonDeserializer(Trade::class.java),
            )

            tradeInput.pipeInput(
                "trade-1",
                Trade("t1", "aapl", "u1", 1L, 10.0, 1L),
            )
            tradeInput.pipeInput(
                "trade-2",
                Trade("t2", "msft", "u1", 20L, 100.0, 2L),
            )

            val outputs = filteredOutput.readValuesToList()

            assertEquals(1, outputs.size)
            assertEquals("MSFT", outputs[0].symbol)
        }
    }

    @Test
    fun clickEventsAreEnrichedFromUserProfileTable() {
        createTopologyTestDriver("step4-click-join").use { driver ->
            val profileInput = driver.createInputTopic(
                "user-profiles",
                Serdes.String().serializer(),
                JsonSerializer<UserProfile>(),
            )
            val clickInput = driver.createInputTopic(
                "click-events",
                Serdes.String().serializer(),
                JsonSerializer<ClickEvent>(),
            )
            val enrichedOutput = driver.createOutputTopic(
                "click-events.enriched",
                Serdes.String().deserializer(),
                JsonDeserializer<ClickWithProfile>(ClickWithProfile::class.java),
            )

            val profile = UserProfile("u-1", "alice", "premium", true)
            val click = ClickEvent("c-1", "u-1", "AAPL", 10L)

            profileInput.pipeInput("u-1", profile)
            clickInput.pipeInput("u-1", click)

            val enriched = enrichedOutput.readValue()

            assertNotNull(enriched)
            assertEquals("alice", enriched.username)
            assertEquals("premium", enriched.tier)
            assertEquals(true, enriched.isVip)
            assertEquals("AAPL", enriched.symbol)
        }
    }

    @Test
    fun clickEventsWithoutProfileKeepDefaultProfileValues() {
        createTopologyTestDriver("step4-click-join-default").use { driver ->
            val clickInput = driver.createInputTopic(
                "click-events",
                Serdes.String().serializer(),
                JsonSerializer<ClickEvent>(),
            )
            val enrichedOutput = driver.createOutputTopic(
                "click-events.enriched",
                Serdes.String().deserializer(),
                JsonDeserializer<ClickWithProfile>(ClickWithProfile::class.java),
            )

            clickInput.pipeInput("u-missing", ClickEvent("c-2", "u-missing", "TSLA", 20L))

            val enriched = enrichedOutput.readValue()

            assertNotNull(enriched)
            assertEquals("unknown", enriched.username)
            assertEquals("guest", enriched.tier)
            assertEquals(false, enriched.isVip)
            assertEquals("u-missing", enriched.userId)
        }
    }
}
