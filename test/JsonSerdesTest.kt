import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class JsonSerdesTest {
    @Test
    fun tradeSerde_roundTripsTrade() {
        val serde = jsonSerde<Trade>()
        val serializer = serde.serializer()
        val deserializer = serde.deserializer()

        val trade = Trade(
            tradeId = "T-1001",
            symbol = "AAPL",
            userId = "user-1",
            quantity = 10,
            price = 175.5,
            tradedAtEpochMillis = 1_700_000_000_000L,
        )

        val bytes = serializer.serialize("trades", trade)
        val restored = deserializer.deserialize("trades", bytes)

        assertNotNull(restored)
        assertEquals(trade, restored)
    }

    @Test
    fun profileSerde_roundTripsUserProfile() {
        val serde = jsonSerde<UserProfile>()
        val serializer = serde.serializer()
        val deserializer = serde.deserializer()

        val profile = UserProfile(
            userId = "user-1",
            username = "alice",
            tier = "premium",
            isVip = true,
        )

        val bytes = serializer.serialize("profiles", profile)
        val restored = deserializer.deserialize("profiles", bytes)

        assertNotNull(restored)
        assertEquals(profile, restored)
    }
}
