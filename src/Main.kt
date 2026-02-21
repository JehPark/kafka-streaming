import org.apache.kafka.common.serialization.Serdes
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
import java.time.Duration
import java.util.Properties
import java.util.concurrent.CountDownLatch

private const val FILTER_THRESHOLD_AMOUNT = 1_000.0

fun buildTradeTopology(
    inputTopic: String,
    filteredOutputTopic: String,
    avgOutputTopic: String,
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
            Materialized.with(Serdes.String(), statsSerde)
        )
        .toStream()
        .selectKey { windowedKey, stats -> windowedKey.key() }
        .to(avgOutputTopic, Produced.with(Serdes.String(), statsSerde))

    return builder.build()
}

fun main() {
    val props = Properties().apply {
        put(StreamsConfig.APPLICATION_ID_CONFIG, "trade-streaming-step3")
        put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        put(
            StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
            Serdes.String()::class.java,
        )
        put(
            StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
            Serdes.String()::class.java,
        )
    }

    val topology = buildTradeTopology(
        inputTopic = "trades",
        filteredOutputTopic = "trades.filtered",
        avgOutputTopic = "trades.symbol.avg",
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
    println("Step 3 stream server started")
    println(topology.describe())

    try {
        shutdownLatch.await()
    } catch (_: InterruptedException) {
        Thread.currentThread().interrupt()
    } finally {
        streams.close()
    }
}
