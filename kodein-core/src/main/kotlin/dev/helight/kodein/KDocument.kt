package dev.helight.kodein

import dev.helight.kodein.spec.BaseDocument
import org.bson.BsonArray
import org.bson.BsonBinary
import org.bson.BsonBoolean
import org.bson.BsonDateTime
import org.bson.BsonDocument
import org.bson.BsonNull
import org.bson.BsonNumber
import org.bson.BsonObjectId
import org.bson.BsonString
import org.bson.BsonValue
import org.bson.types.ObjectId
import kotlin.collections.withIndex
import kotlin.time.Instant

class KDocument(
    val kodein: Kodein,
    val bson: BsonDocument
) {
    var bsonId: BsonValue?
        get() = bson["_id"]
        set(value) {
            when (value) {
                null -> bson.remove("_id")
                else -> bson["_id"] = value
            }
        }

    var id: ObjectId?
        get() = when (val idValue = bsonId) {
            is BsonObjectId -> idValue.value
            else -> null
        }
        set(value) = when (value) {
            null -> bsonId = null
            else -> bsonId = BsonObjectId(value)
        }

    var stringId: String?
        get() = toStringId(bson)
        set(value) = when (value) {
            null -> bsonId = null
            else -> bsonId = BsonString(value)
        }

    fun getEmbedded(path: String): BsonValue? = bson.getEmbedded(path)
    fun getEmbedded(path: Iterable<String>): BsonValue? = bson.getEmbedded(path)

    fun get(path: String): BsonValue? = bson[path]

    fun getString(path: String): String? {
        val value = getEmbedded(path) ?: return null
        if (value is BsonNull) return null
        return if (value is BsonString) value.value else throw IllegalArgumentException("Field $path is not a String")
    }

    fun getInt(path: String): Int? {
        val value = getEmbedded(path) ?: return null
        if (value is BsonNull) return null
        return if (value is BsonNumber) value.intValue() else throw IllegalArgumentException("Field $path is not an Number")
    }

    fun getLong(path: String): Long? {
        val value = getEmbedded(path) ?: return null
        if (value is BsonNull) return null
        return if (value is BsonNumber) value.longValue() else throw IllegalArgumentException("Field $path is not an Number")
    }

    fun getDouble(path: String): Double? {
        val value = getEmbedded(path) ?: return null
        if (value is BsonNull) return null
        return if (value is BsonNumber) value.doubleValue() else throw IllegalArgumentException("Field $path is not a Number")
    }

    fun getBoolean(path: String): Boolean? {
        val value = getEmbedded(path) ?: return null
        if (value is BsonNull) return null
        return if (value is BsonBoolean) value.value else throw IllegalArgumentException("Field $path is not a Boolean")
    }

    fun getBinary(path: String): ByteArray? {
        val value = getEmbedded(path) ?: return null
        if (value is BsonNull) return null
        return if (value is BsonBinary) value.data else throw IllegalArgumentException("Field $path is not a Binary")
    }

    fun getDateTime(path: String): Instant? {
        val value = getEmbedded(path) ?: return null
        if (value is BsonNull) return null
        return if (value is BsonDateTime) Instant.fromEpochMilliseconds(value.value) else throw IllegalArgumentException(
            "Field $path is not a DateTime"
        )
    }

    fun getDocument(path: String): KDocument? {
        val value = getEmbedded(path) ?: return null
        if (value is BsonNull) return null
        return if (value is BsonDocument) KDocument(kodein, value)
        else throw IllegalArgumentException("Field $path is not a Document")
    }

    fun <T> getClass(path: String, clazz: Class<T>): T? {
        val value = getEmbedded(path) ?: return null
        if (value is BsonNull) return null
        if (value !is BsonDocument) throw IllegalArgumentException("Field $path is not a Document")
        return kodein.decode(value, clazz)
    }

    inline fun <reified T> getClass(path: String): T? = getClass(path, T::class.java)

    fun <T> asClass(clazz: Class<T>): T {
        val decoded = kodein.decode(bson, clazz)
        if (decoded is BaseDocument) {
            decoded.bsonId = bson["_id"]
            decoded.document = this
        }
        return decoded
    }
    inline fun <reified T> asClass(): T = asClass(T::class.java)

    fun <T> getClassList(path: String, clazz: Class<T>): List<T>? {
        val value = getEmbedded(path) ?: return null
        if (value is BsonNull) return null
        if (value !is BsonArray) throw IllegalArgumentException("Field $path is not an Array")
        val list = mutableListOf<T>()
        for ((index, item) in value.withIndex()) {
            if (item !is BsonDocument) throw IllegalArgumentException("Array item at $index is not a Document")
            val obj = kodein.decode(item, clazz)
            list.add(obj)
        }
        return list
    }

    inline fun <reified T> getClassList(path: String): List<T>? = getClassList(path, T::class.java)

    override fun toString(): String {
        return "KDocument(bson=$bson)"
    }
}

