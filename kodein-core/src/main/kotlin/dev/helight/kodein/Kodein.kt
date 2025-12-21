package dev.helight.kodein

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.builtins.ByteArraySerializer
import kotlinx.serialization.cbor.Cbor
import kotlinx.serialization.decodeFromByteArray
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.buildSerialDescriptor
import kotlinx.serialization.encodeToByteArray
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.overwriteWith
import org.bson.*
import org.bson.codecs.*
import org.bson.codecs.configuration.CodecRegistries
import org.bson.codecs.configuration.CodecRegistry
import org.bson.codecs.kotlinx.KotlinSerializerCodecProvider
import org.bson.codecs.kotlinx.defaultSerializersModule
import org.bson.io.BasicOutputBuffer
import org.bson.io.ByteBufferBsonInput
import org.bson.json.JsonMode
import org.bson.json.JsonWriterSettings
import java.nio.ByteBuffer
import kotlin.time.Instant

@OptIn(ExperimentalSerializationApi::class)
class Kodein(
    val serializersModule: SerializersModule = DEFAULT_SERIALIZERS_MODULE,
    val codecRegistry: CodecRegistry = DEFAULT_CODEC_REGISTRY,
) {

    private val documentCodec = DocumentCodec(codecRegistry)
    private val bsonDocumentCodec = BsonDocumentCodec()

    fun encodeDocument(document: Document): BsonDocument {
        return document.toBsonDocument(BsonDocument::class.java, codecRegistry)
    }

    fun decodeDocument(bsonDocument: BsonDocument): Document {
        val reader = BsonDocumentReader(bsonDocument)
        return documentCodec.decode(reader, DecoderContext.builder().build())
    }

    fun encodeDocumentBinary(document: BsonDocument): ByteArray {
        val bsonArrayOutput = BasicOutputBuffer()
        val writer = BsonBinaryWriter(bsonArrayOutput)
        bsonDocumentCodec.encode(writer, document, EncoderContext.builder().build())
        return bsonArrayOutput.toByteArray()
    }

    fun decodeDocumentBinary(data: ByteArray): BsonDocument {
        val buffer = ByteBufNIO(ByteBuffer.wrap(data))
        val input = ByteBufferBsonInput(buffer)
        val reader = BsonBinaryReader(input)
        return bsonDocumentCodec.decode(reader, DecoderContext.builder().build())
    }

    fun introspect(document: BsonDocument): KDocument {
        return KDocument(this, document)
    }

    fun <T> decode(document: BsonDocument, clazz: Class<T>): T {
        val innerValue = document["_v"]
        if (innerValue != null) {
            @Suppress("UNCHECKED_CAST") return decodeValue(innerValue) as T
        }
        val decoder = codecRegistry.get(clazz)
        val reader = BsonDocumentReader(document)
        return decoder.decode(reader, DecoderContext.builder().build())
    }

    fun <T> decodeBinary(data: ByteArray, clazz: Class<T>): T {
        val buffer = ByteBufNIO(ByteBuffer.wrap(data))
        val input = ByteBufferBsonInput(buffer)
        val reader = BsonBinaryReader(input)
        val document = bsonDocumentCodec.decode(reader, DecoderContext.builder().build())
        return decode(document, clazz)
    }

    inline fun <reified T> decode(document: BsonDocument): T {
        return decode(document, T::class.java)
    }

    inline fun <reified T> decodeBinary(data: ByteArray): T {
        return decodeBinary(data, T::class.java)
    }

    fun <T> encode(obj: T, clazz: Class<T>): BsonDocument {
        encodeValue(obj)?.let {
            val bsonDocument = BsonDocument()
            bsonDocument["_v"] = it
            return bsonDocument
        }
        val encoder = codecRegistry.get(clazz)
        val bsonDocument = BsonDocument()
        val writer = BsonDocumentWriter(bsonDocument)
        encoder.encode(writer, obj, EncoderContext.builder().build())
        return bsonDocument
    }

    fun <T> encodeBinary(obj: T, clazz: Class<T>): ByteArray {
        val document = encode(obj, clazz)
        val bsonArrayOutput = BasicOutputBuffer()
        val writer = BsonBinaryWriter(bsonArrayOutput)
        bsonDocumentCodec.encode(writer, document, EncoderContext.builder().build())
        return bsonArrayOutput.toByteArray()
    }

    inline fun <reified T> encode(obj: T): BsonDocument {
        return encode(obj, T::class.java)
    }

    inline fun <reified T> encodeBinary(obj: T): ByteArray {
        return encodeBinary(obj, T::class.java)
    }

    private fun encodeValue(obj: Any?): BsonValue? {
        return when (obj) {
            null -> BsonNull.VALUE
            is String -> BsonString(obj)
            is Int -> BsonInt32(obj)
            is Long -> BsonInt64(obj)
            is Boolean -> BsonBoolean(obj)
            is Double -> BsonDouble(obj)
            is ByteArray -> BsonBinary(obj)
            is Instant -> BsonDateTime(obj.toEpochMilliseconds())
            else -> null
        }
    }

    private fun decodeValue(value: BsonValue): Any? {
        return when (value) {
            is BsonString -> value.value
            is BsonInt32 -> value.value
            is BsonInt64 -> value.value
            is BsonBoolean -> value.value
            is BsonDouble -> value.value
            is BsonBinary -> value.data
            is BsonDateTime -> Instant.fromEpochMilliseconds(value.value)
            is BsonNull -> null
            else -> throw IllegalArgumentException("Unsupported BsonValue type for decodeValue: ${value.javaClass}")
        }
    }

    companion object {
        @OptIn(ExperimentalSerializationApi::class)
        val DEFAULT_SERIALIZERS_MODULE: SerializersModule = SerializersModule {
            this.include(defaultSerializersModule)
        }

        val DEFAULT_CODEC_REGISTRY: CodecRegistry =
            CodecRegistries.fromProviders(KotlinSerializerCodecProvider(DEFAULT_SERIALIZERS_MODULE))
    }
}

private class JsonBsonDocumentInteropMapper(
    val mapper: Kodein
) : KSerializer<BsonDocument> {

    val delegatedSerializer = JsonElement.serializer()

    override val descriptor: SerialDescriptor
        get() {
            return SerialDescriptor("kodein.BsonDocument", delegatedSerializer.descriptor)
        }

    private val jsonWriterSettings = JsonWriterSettings.builder().outputMode(JsonMode.STRICT).build()

    override fun serialize(encoder: Encoder, value: BsonDocument) {
        val element = Json.parseToJsonElement(value.toJson(jsonWriterSettings))
        delegatedSerializer.serialize(encoder, element)
    }

    override fun deserialize(decoder: Decoder): BsonDocument {
        val element = delegatedSerializer.deserialize(decoder)
        val jsonString = Json.encodeToString(element)
        return BsonDocument.parse(jsonString)
    }
}

private class CborBsonDocumentInteropMapper(
    val mapper: Kodein
) : KSerializer<BsonDocument> {

    val delegatedSerializer = ByteArraySerializer()

    override val descriptor: SerialDescriptor
        get() {
            return SerialDescriptor("kodein.BsonDocument", delegatedSerializer.descriptor)
        }

    override fun serialize(encoder: Encoder, value: BsonDocument) {
        val element = mapper.encodeDocumentBinary(value)
        delegatedSerializer.serialize(encoder, element)
    }

    override fun deserialize(decoder: Decoder): BsonDocument {
        val element = delegatedSerializer.deserialize(decoder)
        return mapper.decodeBinary(element)
    }
}