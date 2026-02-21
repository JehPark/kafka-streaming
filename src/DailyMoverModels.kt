data class DailyCandlestick(
    val symbol: String = "",
    val dateKey: String = "",
    val open: Double = 0.0,
    val close: Double = 0.0,
    val openTsEpochMillis: Long = Long.MAX_VALUE,
    val closeTsEpochMillis: Long = Long.MIN_VALUE,
    val totalQuantity: Long = 0,
    val totalTrades: Long = 0,
)

data class DailyMovement(
    val symbol: String = "",
    val dateKey: String = "",
    val open: Double = 0.0,
    val close: Double = 0.0,
    val openTsEpochMillis: Long = 0,
    val closeTsEpochMillis: Long = 0,
    val direction: String = "UP",
    val moveRate: Double = 0.0,
    val volume: Long = 0,
    val tradeCount: Long = 0,
)

data class DailyTopNState(
    val dateKey: String = "",
    val direction: String = "UP",
    val symbolToMovement: Map<String, DailyMovement> = emptyMap(),
)

data class DailyTopNOutput(
    val dateKey: String = "",
    val direction: String = "UP",
    val items: List<DailyMovement> = emptyList(),
    val updatedAtEpochMillis: Long = 0,
)
