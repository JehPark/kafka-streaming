data class Trade(
    val tradeId: String,
    val symbol: String,
    val userId: String,
    val quantity: Long,
    val price: Double,
    val tradedAtEpochMillis: Long,
)

data class TradeStats(
    val symbol: String,
    val totalTrades: Long,
    val totalQuantity: Long,
    val totalAmount: Double,
    val averagePrice: Double,
)

data class UserProfile(
    val userId: String,
    val username: String,
    val tier: String,
    val isVip: Boolean,
)
