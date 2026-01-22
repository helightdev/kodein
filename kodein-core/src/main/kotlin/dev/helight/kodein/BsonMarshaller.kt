package dev.helight.kodein

import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.double
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.int
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.long
import kotlinx.serialization.json.longOrNull
import org.bson.BsonArray
import org.bson.BsonBinary
import org.bson.BsonBoolean
import org.bson.BsonDateTime
import org.bson.BsonDocument
import org.bson.BsonDouble
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonNull
import org.bson.BsonObjectId
import org.bson.BsonString
import org.bson.BsonValue
import org.bson.types.ObjectId
import kotlin.collections.component1
import kotlin.collections.component2
import kotlin.collections.forEach
import kotlin.collections.iterator
import kotlin.time.Instant

object BsonMarshaller {
    fun unmarshal(bsonValue: BsonValue): Any? {
        return when (bsonValue) {
            is BsonDocument -> LinkedHashMap<String, Any?>(bsonValue.size).apply {
                bsonValue.forEach { (key, value) ->
                    this[key] = unmarshal(value)
                }
            }

            is BsonArray -> ArrayList<Any?>(bsonValue.size).apply {
                bsonValue.forEach { item ->
                    this.add(unmarshal(item))
                }
            }

            is BsonString -> bsonValue.value
            is BsonInt32 -> bsonValue.value
            is BsonInt64 -> bsonValue.value
            is BsonDouble -> bsonValue.value
            is BsonBoolean -> bsonValue.value
            is BsonBinary -> bsonValue.data
            is BsonObjectId -> bsonValue.value
            is BsonDateTime -> Instant.fromEpochMilliseconds(bsonValue.value)
            is BsonNull -> null
            else -> throw IllegalArgumentException("Unsupported BsonValue type: ${bsonValue::class}")
        }
    }

    fun marshal(value: Any?): BsonValue {
        return marshalOrNull(value) ?: throw IllegalArgumentException("Unsupported type: ${value!!::class}")
    }

    fun marshalOrNull(value: Any?): BsonValue? {
        if (value is BsonValue) return value
        return when (value) {
            is Map<*, *> -> BsonDocument().apply { for ((k, v) in value) put(k.toString(), marshal(v)) }
            is Collection<*> -> BsonArray().apply { for (item in value) add(marshal(item)) }
            is String -> BsonString(value)
            is Int -> BsonInt32(value)
            is Long -> BsonInt64(value)
            is Double -> BsonDouble(value)
            is Boolean -> BsonBoolean(value)
            is ByteArray -> BsonBinary(value)
            is Instant -> BsonDateTime(value.toEpochMilliseconds())
            is ObjectId -> BsonObjectId(value)
            null -> BsonNull.VALUE
            else -> null
        }
    }

    fun <T> marshal(value: T?, clazz: Class<T>, kodein: Kodein?): BsonValue {
        val bsonValue = marshalOrNull(value)
        if (bsonValue != null) return bsonValue
        requireNotNull(kodein) { "Kodein instance is required for custom type serialization." }
        return kodein.encode(value!!, clazz)
    }

    fun JsonElement.toBson(specialTypes: Boolean = false): BsonValue {
        return when (this) {
            is JsonNull -> BsonNull.VALUE
            is JsonArray -> {
                val bsonArray = BsonArray()
                this.forEach { element ->
                    bsonArray.add(element.toBson(specialTypes))
                }
                bsonArray
            }

            is JsonObject -> {
                val bsonDocument = BsonDocument()
                this.entries.forEach { (key, value) ->
                    bsonDocument.append(key, value.toBson(specialTypes))
                }
                bsonDocument
            }

            is JsonPrimitive -> {
                when {
                    this.isString -> {
                        if (specialTypes && isoTimestampRegex.matches(this.content)) {
                            Instant.parseOrNull(this.content)?.let {
                                return BsonDateTime(it.toEpochMilliseconds())
                            }
                        }
                        BsonString(this.content)
                    }

                    this.booleanOrNull != null -> BsonBoolean(this.boolean)
                    this.intOrNull != null -> BsonInt32(this.int)
                    this.longOrNull != null -> BsonInt64(this.long)
                    this.doubleOrNull != null -> BsonDouble(this.double)
                    else -> throw IllegalArgumentException("Unsupported JsonPrimitive type for conversion to BsonValue")
                }
            }
        }
    }

    private val isoTimestampRegex = Regex("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?Z$")
}