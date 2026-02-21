import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TopologyTestDriver
import kotlin.math.abs
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import java.time.LocalDateTime
import java.time.ZoneOffset

class TopologyStep6Test {
    private fun createDailyMoverTopologyTestDriver(applicationId: String): TopologyTestDriver {
        val topology = buildDailyMoverTopology(
            tradeInputTopic = "trades",
            topGainersOutputTopic = "daily.movers.gainers",
            topLosersOutputTopic = "daily.movers.losers",
        )

        val props = java.util.Properties().apply {
            putAll(buildStreamsProperties(applicationId))
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092")
            put(StreamsConfig.STATE_DIR_CONFIG, "build/test-state/$applicationId")
        }

        return TopologyTestDriver(topology, props)
    }

    @Test
    fun dailyOpenCloseBuildsMovementFromOutOfOrderEvents() {
        createDailyMoverTopologyTestDriver("step6-open-close").use { driver ->
            val tradeInput = driver.createInputTopic(
                "trades",
                Serdes.String().serializer(),
                JsonSerializer<Trade>(),
            )

            val gainersOutput = driver.createOutputTopic(
                "daily.movers.gainers",
                Serdes.String().deserializer(),
                JsonDeserializer<DailyTopNOutput>(DailyTopNOutput::class.java),
            )

            val losersOutput = driver.createOutputTopic(
                "daily.movers.losers",
                Serdes.String().deserializer(),
                JsonDeserializer<DailyTopNOutput>(DailyTopNOutput::class.java),
            )

            tradeInput.pipeInput("trade-1", Trade("t1", "goog", "u1", 1L, 100.0, ts(2026, 2, 21, 9, 0, 0)), ts(2026, 2, 21, 9, 0, 0))
            tradeInput.pipeInput("trade-2", Trade("t2", "goog", "u1", 1L, 120.0, ts(2026, 2, 21, 9, 1, 0)), ts(2026, 2, 21, 9, 1, 0))
            // out-of-order arrival for earliest trade
            tradeInput.pipeInput("trade-3", Trade("t3", "goog", "u1", 1L, 90.0, ts(2026, 2, 21, 8, 50, 0)), ts(2026, 2, 21, 8, 50, 0))

            val gainers = gainersOutput.readValuesToList()
            val losers = losersOutput.readValuesToList()

            val latestGainer = gainers.lastOrNull()
            assertNotNull(latestGainer)
            assertTrue(latestGainer.items.isNotEmpty())

            val goog = latestGainer.items.find { it.symbol == "GOOG" }
            assertNotNull(goog)
            assertEquals(90.0, goog.open)
            assertEquals(120.0, goog.close)
            assertTrue(abs(goog.moveRate - ((120.0 - 90.0) / 90.0 * 100.0)) < 0.0001)

            assertTrue(losers.isEmpty())
        }
    }

    @Test
    fun dailyTopMovers_outputsGainersByDescendingRateAndTop10Limit() {
        createDailyMoverTopologyTestDriver("step6-top10").use { driver ->
            val tradeInput = driver.createInputTopic(
                "trades",
                Serdes.String().serializer(),
                JsonSerializer<Trade>(),
            )

            val gainersOutput = driver.createOutputTopic(
                "daily.movers.gainers",
                Serdes.String().deserializer(),
                JsonDeserializer<DailyTopNOutput>(DailyTopNOutput::class.java),
            )

            val openBaseTs = ts(2026, 2, 22, 9, 0, 0)

            for (i in 1..11) {
                val symbol = "S%02d".format(i)
                val openTs = openBaseTs + i * 1_000L
                val closeTs = openTs + 10L

                tradeInput.pipeInput(
                    "open-$symbol",
                    Trade("t-open-$symbol", symbol, "u1", 1L, 100.0, openTs),
                    openTs,
                )
                tradeInput.pipeInput(
                    "close-$symbol",
                    Trade("t-close-$symbol", symbol, "u1", 1L, 100.0 + i, closeTs),
                    closeTs,
                )
            }

            val latest = gainersOutput.readValuesToList().lastOrNull()
            assertNotNull(latest)
            assertEquals("UP", latest.direction)
            assertEquals(10, latest.items.size)
            assertEquals("S11", latest.items.first().symbol)
            assertEquals("S02", latest.items.last().symbol)

            val symbols = latest.items.map { it.symbol }
            assertEquals(symbols.toSet().size, symbols.size)
        }
    }

    @Test
    fun dailyTopMovers_outputsLosersByAscendingRate() {
        createDailyMoverTopologyTestDriver("step6-losers").use { driver ->
            val tradeInput = driver.createInputTopic(
                "trades",
                Serdes.String().serializer(),
                JsonSerializer<Trade>(),
            )

            val losersOutput = driver.createOutputTopic(
                "daily.movers.losers",
                Serdes.String().deserializer(),
                JsonDeserializer<DailyTopNOutput>(DailyTopNOutput::class.java),
            )

            tradeInput.pipeInput("a-open", Trade("t-a", "aapl", "u1", 1L, 100.0, ts(2026, 2, 23, 9, 0, 0)), ts(2026, 2, 23, 9, 0, 0))
            tradeInput.pipeInput("a-close", Trade("t-a2", "aapl", "u1", 1L, 70.0, ts(2026, 2, 23, 10, 0, 0)), ts(2026, 2, 23, 10, 0, 0))
            tradeInput.pipeInput("m-open", Trade("t-m", "msft", "u1", 1L, 100.0, ts(2026, 2, 23, 9, 5, 0)), ts(2026, 2, 23, 9, 5, 0))
            tradeInput.pipeInput("m-close", Trade("t-m2", "msft", "u1", 1L, 95.0, ts(2026, 2, 23, 10, 5, 0)), ts(2026, 2, 23, 10, 5, 0))

            val latest = losersOutput.readValuesToList().lastOrNull()
            assertNotNull(latest)
            assertEquals("DOWN", latest.direction)
            assertTrue(latest.items.size >= 2)

            val ranked = latest.items
            assertEquals("AAPL", ranked.first().symbol)
        }
    }

    @Test
    fun dailyTopMovers_separatesMoversByUtcDateWindow() {
        createDailyMoverTopologyTestDriver("step6-boundary").use { driver ->
            val tradeInput = driver.createInputTopic(
                "trades",
                Serdes.String().serializer(),
                JsonSerializer<Trade>(),
            )

            val gainersOutput = driver.createOutputTopic(
                "daily.movers.gainers",
                Serdes.String().deserializer(),
                JsonDeserializer<DailyTopNOutput>(DailyTopNOutput::class.java),
            )

            tradeInput.pipeInput("day1-open", Trade("t1", "tsla", "u1", 1L, 100.0, ts(2026, 2, 20, 23, 55, 0)), ts(2026, 2, 20, 23, 55, 0))
            tradeInput.pipeInput("day1-close", Trade("t2", "tsla", "u1", 1L, 120.0, ts(2026, 2, 20, 23, 58, 0)), ts(2026, 2, 20, 23, 58, 0))
            tradeInput.pipeInput("day2-open", Trade("t3", "tsla", "u1", 1L, 90.0, ts(2026, 2, 21, 0, 1, 0)), ts(2026, 2, 21, 0, 1, 0))
            tradeInput.pipeInput("day2-close", Trade("t4", "tsla", "u1", 1L, 110.0, ts(2026, 2, 21, 0, 3, 0)), ts(2026, 2, 21, 0, 3, 0))

            val outputs = gainersOutput.readValuesToList()
            val byDate = outputs.groupBy { it.dateKey }

            assertNotNull(byDate["2026-02-20"])
            assertNotNull(byDate["2026-02-21"])

            val day1Top = byDate["2026-02-20"]!!.last()
            val day2Top = byDate["2026-02-21"]!!.last()

            assertEquals("UP", day1Top.direction)
            assertEquals("UP", day2Top.direction)
            assertEquals("TSLA", day1Top.items.first().symbol)
            assertEquals("TSLA", day2Top.items.first().symbol)
            assertEquals(20.0, day1Top.items.first().moveRate)
            assertEquals(((110.0 - 90.0) / 90.0) * 100.0, day2Top.items.first().moveRate)
        }
    }

    @Test
    fun dailyMoverTopologyConfigurationShouldKeepStepDefaults() {
        val props = buildStreamsProperties("step6-config")

        assertEquals("step6-config", props[StreamsConfig.APPLICATION_ID_CONFIG])
        assertEquals(StreamsConfig.OPTIMIZE, props[StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG])
        assertEquals(StreamsConfig.EXACTLY_ONCE_V2, props[StreamsConfig.PROCESSING_GUARANTEE_CONFIG])
    }
}

private fun ts(year: Int, month: Int, day: Int, hour: Int, minute: Int, second: Int): Long {
    return LocalDateTime
        .of(year, month, day, hour, minute, second)
        .toInstant(ZoneOffset.UTC)
        .toEpochMilli()
}
