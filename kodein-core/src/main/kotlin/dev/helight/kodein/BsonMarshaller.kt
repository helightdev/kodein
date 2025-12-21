package dev.helight.kodein

import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
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
            else -> throw IllegalArgumentException("Unsupported type: ${value::class}")
        }
    }
}