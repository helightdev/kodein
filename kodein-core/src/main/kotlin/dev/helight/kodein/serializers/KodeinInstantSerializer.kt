package dev.helight.kodein.serializers

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import org.bson.BsonDateTime
import org.bson.codecs.kotlinx.BsonDecoder
import org.bson.codecs.kotlinx.BsonEncoder
import kotlin.time.Instant

typealias KInstant = @Serializable(with = KodeinInstantSerializer::class) Instant

@OptIn(ExperimentalSerializationApi::class)
internal object KodeinInstantSerializer : KSerializer<Instant> {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("kodein.Instant", PrimitiveKind.STRING)

    override fun serialize(encoder: Encoder, value: Instant) {
        if (encoder is BsonEncoder) {
            encoder.encodeBsonValue(BsonDateTime(value.toEpochMilliseconds()))
            return
        }

        encoder.encodeString(value.toString())
    }

    override fun deserialize(decoder: Decoder): Instant {
        if (decoder is BsonDecoder) {
            val value = decoder.decodeBsonValue()
            if (value.isDateTime) {
                return Instant.fromEpochMilliseconds(value.asDateTime().value)
            } else if (value.isString) {
                return Instant.parse(value.asString().value)
            }
        }
        return Instant.parse(decoder.decodeString())
    }
}