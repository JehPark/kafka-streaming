import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Aggregator
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.kstream.Grouped
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.TimeWindows
import org.apache.kafka.streams.kstream.Joined
import org.apache.kafka.streams.processor.TimestampExtractor
import org.apache.kafka.streams.state.WindowStore
import org.apache.kafka.streams.state.KeyValueStore
import java.time.Instant
import java.time.ZoneOffset
import java.time.Duration
import java.util.Properties
import java.util.concurrent.CountDownLatch

private const val FILTER_THRESHOLD_AMOUNT = 1_000.0
private const val AGGREGATION_STATE_STORE = "trade-stats-window-store"
private const val CHANGES_LOG_MIN_COMPACTION_LAG_MS = 300_000L
private const val NUM_STANDBY_REPLICAS = 1
private const val WINDOW_SIZE_SECONDS = 5L
private const val WINDOW_ADVANCE_SECONDS = 1L
private const val WINDOW_GRACE_SECONDS = 2L
private const val TOPOLOGY_OPTIMIZATION_MODE = StreamsConfig.OPTIMIZE
private const val DAILY_CANDLE_STATE_STORE = "daily-candle-store"
private const val DAILY_TOP_GAINERS_STATE_STORE = "daily-top-gainers-store"
private const val DAILY_TOP_LOSERS_STATE_STORE = "daily-top-losers-store"
private const val DAILY_WINDOW_SIZE_HOURS = 24L
private const val DAILY_WINDOW_GRACE_MINUTES = 5L
private const val DAILY_TOP_N = 10

class TradeTimestampExtractor : TimestampExtractor {
    override fun extract(record: ConsumerRecord<Any?, Any?>, previousTimestamp: Long): Long {
        val trade = record.value() as? Trade ?: return record.timestamp()
        return trade.tradedAtEpochMillis
    }
}

fun buildTradeTopology(
    inputTopic: String,
    filteredOutputTopic: String,
    avgOutputTopic: String,
    userProfileTopic: String,
    clickEventTopic: String,
    enrichedClickOutputTopic: String,
): Topology {
    val builder = StreamsBuilder()

    val tradeSerde = jsonSerde<Trade>()
    val statsSerde = jsonSerde<TradeStats>()
    val tradeStream: KStream<String, Trade> = builder.stream(
        inputTopic,
        Consumed.with<String, Trade>(
            Serdes.String(),
            tradeSerde,
            TradeTimestampExtractor(),
            Topology.AutoOffsetReset.EARLIEST,
        ),
    )

    val filtered = tradeStream
        .filter { _, trade -> trade.price * trade.quantity >= FILTER_THRESHOLD_AMOUNT }
        .mapValues { trade ->
            trade.copy(
                symbol = trade.symbol.uppercase(),
                tradedAtEpochMillis = trade.tradedAtEpochMillis,
            )
        }

    filtered.to(filteredOutputTopic, Produced.with(Serdes.String(), tradeSerde))

    filtered
        .selectKey { _, trade -> trade.symbol }
        .groupByKey(Grouped.with(Serdes.String(), tradeSerde))
        .windowedBy(
            TimeWindows.ofSizeAndGrace(
                Duration.ofSeconds(WINDOW_SIZE_SECONDS),
                Duration.ofSeconds(WINDOW_GRACE_SECONDS),
            ).advanceBy(Duration.ofSeconds(WINDOW_ADVANCE_SECONDS)),
        )
        .aggregate(
            { TradeStats("", 0, 0, 0.0, 0.0) },
            Aggregator { symbol, trade, stats ->
                val updatedQuantity = stats.totalQuantity + trade.quantity
                val updatedAmount = stats.totalAmount + (trade.price * trade.quantity)
                val avgPrice = if (updatedQuantity == 0L) 0.0 else updatedAmount / updatedQuantity

                TradeStats(
                    symbol = symbol,
                    totalTrades = stats.totalTrades + 1,
                    totalQuantity = updatedQuantity,
                    totalAmount = updatedAmount,
                    averagePrice = avgPrice,
                )
            },
            Materialized.`as`<String, TradeStats, WindowStore<Bytes, ByteArray>>(AGGREGATION_STATE_STORE)
                .withKeySerde(Serdes.String())
                .withValueSerde(statsSerde)
                .withLoggingEnabled(
                    mapOf(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG to CHANGES_LOG_MIN_COMPACTION_LAG_MS.toString()),
                ),
        )
        .toStream()
        .selectKey { windowedKey, stats -> windowedKey.key() }
        .to(avgOutputTopic, Produced.with(Serdes.String(), statsSerde))

    val profileTable: KTable<String, UserProfile> =
        builder.table(userProfileTopic, Materialized.with(Serdes.String(), jsonSerde<UserProfile>()))
    val clickStream: KStream<String, ClickEvent> =
        builder.stream(
            clickEventTopic,
            Consumed.with<String, ClickEvent>(Serdes.String(), jsonSerde<ClickEvent>()),
        )

    clickStream.leftJoin(
        profileTable,
        { click, profile ->
            ClickWithProfile(
                clickId = click.clickId,
                userId = click.userId,
                symbol = click.symbol,
                clickedAtEpochMillis = click.clickedAtEpochMillis,
                username = profile?.username ?: "unknown",
                tier = profile?.tier ?: "guest",
                isVip = profile?.isVip ?: false,
            )
        },
        Joined.with(
            Serdes.String(),
            jsonSerde<ClickEvent>(),
            jsonSerde<UserProfile>(),
        ),
    ).to(enrichedClickOutputTopic, Produced.with(Serdes.String(), jsonSerde<ClickWithProfile>()))
    
    return builder.build()
}

fun buildDailyMoverTopology(
    tradeInputTopic: String,
    topGainersOutputTopic: String,
    topLosersOutputTopic: String,
): Topology {
    val builder = StreamsBuilder()
    val tradeSerde = jsonSerde<Trade>()
    val movementSerde = jsonSerde<DailyMovement>()
    val topNStateSerde = jsonSerde<DailyTopNState>()
    val topNOutputSerde = jsonSerde<DailyTopNOutput>()

    val tradeStream: KStream<String, Trade> = builder.stream(
        tradeInputTopic,
        Consumed.with<String, Trade>(
            Serdes.String(),
            tradeSerde,
            TradeTimestampExtractor(),
            Topology.AutoOffsetReset.EARLIEST,
        ),
    )

    val dailyMovementStream: KStream<String, DailyMovement> = tradeStream
        .mapValues { trade ->
            trade.copy(
                symbol = trade.symbol.uppercase(),
                tradedAtEpochMillis = trade.tradedAtEpochMillis,
            )
        }
        .selectKey { _, trade -> trade.symbol }
        .groupByKey(Grouped.with(Serdes.String(), tradeSerde))
        .windowedBy(
            TimeWindows.ofSizeAndGrace(
                Duration.ofHours(DAILY_WINDOW_SIZE_HOURS),
                Duration.ofMinutes(DAILY_WINDOW_GRACE_MINUTES),
            ),
        )
        .aggregate(
            { DailyCandlestick() },
            { symbol, trade, candle ->
                buildDailyCandlestick(
                    symbol = symbol,
                    trade = trade,
                    current = candle,
                )
            },
            Materialized.`as`<String, DailyCandlestick, WindowStore<Bytes, ByteArray>>(DAILY_CANDLE_STATE_STORE)
                .withKeySerde(Serdes.String())
                .withValueSerde(jsonSerde<DailyCandlestick>())
                .withLoggingEnabled(
                    mapOf(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG to CHANGES_LOG_MIN_COMPACTION_LAG_MS.toString()),
                ),
        )
        .toStream()
        .map { windowedKey, candle ->
            val dateKey =
                if (candle.dateKey.isNotBlank()) {
                    candle.dateKey
                } else {
                    epochMillisToDateKey(windowedKey.window().start())
                }

            KeyValue(
                dateKey,
                toDailyMovement(candle.copy(symbol = windowedKey.key(), dateKey = dateKey)),
            )
        }

    dailyMovementStream
        .filter { _, movement -> movement.direction == "UP" }
        .groupByKey(Grouped.with(Serdes.String(), movementSerde))
        .aggregate(
            { DailyTopNState(direction = "UP") },
            { dateKey, movement, state ->
                buildTopNState(
                    direction = "UP",
                    dateKey = dateKey,
                    movement = movement,
                    state = state,
                )
            },
            Materialized.`as`<String, DailyTopNState, KeyValueStore<Bytes, ByteArray>>(DAILY_TOP_GAINERS_STATE_STORE)
                .withKeySerde(Serdes.String())
                .withValueSerde(topNStateSerde)
                .withLoggingEnabled(
                    mapOf(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG to CHANGES_LOG_MIN_COMPACTION_LAG_MS.toString()),
                ),
        )
        .toStream()
        .mapValues { state ->
            toDailyTopNOutput(state)
        }
        .to(topGainersOutputTopic, Produced.with(Serdes.String(), topNOutputSerde))

    dailyMovementStream
        .filter { _, movement -> movement.direction == "DOWN" }
        .groupByKey(Grouped.with(Serdes.String(), movementSerde))
        .aggregate(
            { DailyTopNState(direction = "DOWN") },
            { dateKey, movement, state ->
                buildTopNState(
                    direction = "DOWN",
                    dateKey = dateKey,
                    movement = movement,
                    state = state,
                )
            },
            Materialized.`as`<String, DailyTopNState, KeyValueStore<Bytes, ByteArray>>(DAILY_TOP_LOSERS_STATE_STORE)
                .withKeySerde(Serdes.String())
                .withValueSerde(topNStateSerde)
                .withLoggingEnabled(
                    mapOf(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG to CHANGES_LOG_MIN_COMPACTION_LAG_MS.toString()),
                ),
        )
        .toStream()
        .mapValues { state ->
            toDailyTopNOutput(state)
        }
        .to(topLosersOutputTopic, Produced.with(Serdes.String(), topNOutputSerde))

    return builder.build()
}

private fun epochMillisToDateKey(epochMillis: Long): String =
    Instant
        .ofEpochMilli(epochMillis)
        .atZone(ZoneOffset.UTC)
        .toLocalDate()
        .toString()

private fun buildDailyCandlestick(
    symbol: String,
    trade: Trade,
    current: DailyCandlestick,
): DailyCandlestick {
    val normalizedSymbol = trade.symbol.uppercase()
    val eventTs = trade.tradedAtEpochMillis
    val dateKey = epochMillisToDateKey(eventTs)
    val isFirstTrade = current.totalTrades == 0L

    val openTs = if (isFirstTrade) eventTs else minOf(current.openTsEpochMillis, eventTs)
    val closeTs = if (isFirstTrade) eventTs else maxOf(current.closeTsEpochMillis, eventTs)
    val open = if (isFirstTrade || eventTs <= openTs) trade.price else current.open
    val close = if (isFirstTrade || eventTs >= closeTs) trade.price else current.close

    return current.copy(
        symbol = if (symbol.isNotBlank()) symbol else normalizedSymbol,
        dateKey = dateKey,
        open = open,
        close = close,
        openTsEpochMillis = openTs,
        closeTsEpochMillis = closeTs,
        totalQuantity = current.totalQuantity + trade.quantity,
        totalTrades = current.totalTrades + 1,
    )
}

private fun toDailyMovement(candle: DailyCandlestick): DailyMovement {
    val moveRate = calculateDailyMoveRate(candle.open, candle.close)

    return DailyMovement(
        symbol = candle.symbol,
        dateKey = candle.dateKey,
        open = candle.open,
        close = candle.close,
        openTsEpochMillis = candle.openTsEpochMillis,
        closeTsEpochMillis = candle.closeTsEpochMillis,
        direction = if (moveRate >= 0.0) "UP" else "DOWN",
        moveRate = moveRate,
        volume = candle.totalQuantity,
        tradeCount = candle.totalTrades,
    )
}

private fun calculateDailyMoveRate(open: Double, close: Double): Double =
    if (open == 0.0) 0.0 else ((close - open) / open) * 100.0

private fun buildTopNState(
    direction: String,
    dateKey: String,
    movement: DailyMovement,
    state: DailyTopNState,
): DailyTopNState {
    val currentBySymbol = state.symbolToMovement.toMutableMap()
    currentBySymbol[movement.symbol] = movement

    val ranked = if (direction == "UP") {
        currentBySymbol.values.sortedByDescending { it.moveRate }
    } else {
        currentBySymbol.values.sortedBy { it.moveRate }
    }.take(DAILY_TOP_N)

    return DailyTopNState(
        dateKey = dateKey,
        direction = direction,
        symbolToMovement = ranked.associateBy { it.symbol },
    )
}

private fun toDailyTopNOutput(state: DailyTopNState): DailyTopNOutput {
    val ranked = if (state.direction == "UP") {
        state.symbolToMovement.values.sortedByDescending { it.moveRate }
    } else {
        state.symbolToMovement.values.sortedBy { it.moveRate }
    }

    val updatedAt = state.symbolToMovement.values.maxByOrNull { it.closeTsEpochMillis }?.closeTsEpochMillis ?: 0L

    return DailyTopNOutput(
        dateKey = state.dateKey,
        direction = state.direction,
        items = ranked,
        updatedAtEpochMillis = updatedAt,
    )
}

fun buildStreamsProperties(applicationId: String): Properties = Properties().apply {
    put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId)
    put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    put(StreamsConfig.STATE_DIR_CONFIG, "build/streams-state/$applicationId")
    put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, TOPOLOGY_OPTIMIZATION_MODE)
    put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, NUM_STANDBY_REPLICAS)
    put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2)
    put(
        StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
        Serdes.String()::class.java,
    )
    put(
        StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        Serdes.String()::class.java,
    )
}

fun defaultTradeStateLogConfig(): Map<String, String> = mapOf(
    TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG to CHANGES_LOG_MIN_COMPACTION_LAG_MS.toString(),
)

fun main() {
    val props = buildStreamsProperties("trade-streaming-step5")

    val topology = buildTradeTopology(
        inputTopic = "trades",
        filteredOutputTopic = "trades.filtered",
        avgOutputTopic = "trades.symbol.avg",
        userProfileTopic = "user-profiles",
        clickEventTopic = "click-events",
        enrichedClickOutputTopic = "click-events.enriched",
    )

    val streams = KafkaStreams(topology, props)
    val shutdownLatch = CountDownLatch(1)

    Runtime.getRuntime().addShutdownHook(
        Thread {
            streams.close(Duration.ofSeconds(10))
            shutdownLatch.countDown()
        }
    )

    streams.start()
    println("Step 4 stream server started")
    println(topology.describe())

    try {
        shutdownLatch.await()
    } catch (_: InterruptedException) {
        Thread.currentThread().interrupt()
    } finally {
        streams.close()
    }
}
