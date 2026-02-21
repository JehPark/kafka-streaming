data class ClickEvent(
    val clickId: String,
    val userId: String,
    val symbol: String,
    val clickedAtEpochMillis: Long,
)

data class ClickWithProfile(
    val clickId: String,
    val userId: String,
    val symbol: String,
    val clickedAtEpochMillis: Long,
    val username: String,
    val tier: String,
    val isVip: Boolean,
)
