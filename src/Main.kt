import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Aggregator
import org.apache.kafka.streams.kstream.Grouped
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.TimeWindows
import org.apache.kafka.streams.kstream.Joined
import org.apache.kafka.streams.state.WindowStore
import java.time.Duration
import java.util.Properties
import java.util.concurrent.CountDownLatch

private const val FILTER_THRESHOLD_AMOUNT = 1_000.0
private const val AGGREGATION_STATE_STORE = "trade-stats-window-store"
private const val CHANGES_LOG_MIN_COMPACTION_LAG_MS = 300_000L
private const val NUM_STANDBY_REPLICAS = 1

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
    val tradeStream = builder.stream(inputTopic, Consumed.with(Serdes.String(), tradeSerde))

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
            TimeWindows.ofSizeAndGrace(Duration.ofSeconds(5), Duration.ZERO)
                .advanceBy(Duration.ofSeconds(1))
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

    val profileTable = builder.table(userProfileTopic, Materialized.with(Serdes.String(), jsonSerde<UserProfile>()))
    val clickStream = builder.stream(clickEventTopic, Consumed.with(Serdes.String(), jsonSerde<ClickEvent>()))

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

fun buildStreamsProperties(applicationId: String): Properties = Properties().apply {
    put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId)
    put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    put(StreamsConfig.STATE_DIR_CONFIG, "build/streams-state/$applicationId")
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
