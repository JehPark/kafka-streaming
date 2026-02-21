import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer
import java.nio.charset.StandardCharsets

private val defaultJsonObjectMapper: ObjectMapper =
    ObjectMapper()
        .registerKotlinModule()
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)

class JsonSerializer<T>(private val objectMapper: ObjectMapper = defaultJsonObjectMapper) : Serializer<T> {
    override fun serialize(topic: String?, data: T?): ByteArray? {
        return data?.let {
            try {
                objectMapper.writeValueAsString(it).toByteArray(StandardCharsets.UTF_8)
            } catch (_: JsonProcessingException) {
                null
            }
        }
    }
}

class JsonDeserializer<T>(
    private val type: Class<T>,
    private val objectMapper: ObjectMapper = defaultJsonObjectMapper,
) : Deserializer<T> {
    override fun deserialize(topic: String?, data: ByteArray?): T? {
        if (data == null) return null

        return try {
            objectMapper.readValue(String(data, StandardCharsets.UTF_8), type)
        } catch (_: Exception) {
            null
        }
    }
}

class JsonSerde<T>(
    private val type: Class<T>,
    private val objectMapper: ObjectMapper = defaultJsonObjectMapper,
) : Serde<T> {
    override fun serializer(): Serializer<T> = JsonSerializer(objectMapper)

    override fun deserializer(): Deserializer<T> = JsonDeserializer(type, objectMapper)
}

inline fun <reified T : Any> jsonSerde(): Serde<T> = JsonSerde(T::class.java)
