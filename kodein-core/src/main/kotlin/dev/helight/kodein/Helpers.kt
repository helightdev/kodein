package dev.helight.kodein

import org.bson.BsonBoolean
import org.bson.BsonDateTime
import org.bson.BsonDocument
import org.bson.BsonDouble
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonNumber
import org.bson.BsonObjectId
import org.bson.BsonString
import org.bson.BsonType
import org.bson.BsonValue
import org.bson.types.ObjectId
import kotlin.collections.component1
import kotlin.collections.component2
import kotlin.collections.set
import kotlin.text.isNotEmpty
import kotlin.time.Instant

internal fun splitPath(path: String): Sequence<String> {
    return path.splitToSequence('.').filter { it.isNotEmpty() }
}

internal fun BsonDocument.constructiveBaseHead(sequence: Sequence<String>): Pair<BsonDocument, String>? {
    val iterator = sequence.iterator()
    if (!iterator.hasNext()) return null

    var current: BsonDocument = this
    var head = iterator.next()

    while (iterator.hasNext()) {
        val key = head
        head = iterator.next()

        val next = current[key]
        current = if (next is BsonDocument) {
            next
        } else {
            val newDoc = BsonDocument()
            current[key] = newDoc
            newDoc
        }
    }
    return Pair(current, head)
}

internal fun BsonDocument.putEmbedded(path: String, value: BsonValue) {
    val (current, head) = constructiveBaseHead(splitPath(path)) ?: return
    current[head] = value
}

internal fun BsonDocument.incEmbedded(path: String, increment: Number) {
    val (current, head) = constructiveBaseHead(splitPath(path)) ?: return

    val currentValue = current[head]
    val newValue = if (currentValue != null) {
        incrementBsonValue(currentValue, increment)
    } else {
        when (increment) {
            is Int -> BsonInt32(increment)
            is Long -> BsonInt64(increment)
            is Double -> BsonDouble(increment)
            else -> throw IllegalArgumentException("Unsupported increment type: ${increment::class}")
        }
    }

    current[head] = newValue
}


internal fun BsonDocument.unsetEmbedded(path: String) {
    val iterator = splitPath(path).iterator()

    if (!iterator.hasNext()) return

    var current: BsonDocument = this
    var head = iterator.next()

    while (iterator.hasNext()) {
        val key = head
        head = iterator.next()

        val next = current[key]
        if (next !is BsonDocument) return
        current = next
    }

    current.remove(head)
}


internal fun BsonDocument.putAllEmbedded(other: BsonDocument) {
    if (other.hasPathFields()) {
        other.forEach { (key, value) ->
            this.putEmbedded(key, value)
        }
    } else {
        other.forEach { (key, value) ->
            this[key] = value
        }
    }
}

internal fun BsonDocument.hasPathFields() = this.keys.any { it.contains('.') }

internal fun BsonDocument.getEmbedded(path: String): BsonValue? {
    if (path.isBlank()) return this
    return getEmbedded(splitPath(path).asIterable())
}

internal fun BsonDocument.getEmbedded(path: Iterable<String>): BsonValue? {
    var current: BsonValue = this
    for (part in path) {
        if (current !is BsonDocument) return null
        current = current[part] ?: return null
    }
    return current
}

internal fun compareBsonValues(a: BsonValue?, b: BsonValue?): Int {
    if (a == null && b == null) return 0
    if (a == null) return -1
    if (b == null) return 1
    if (a.bsonType == b.bsonType) return when (a.bsonType) {
        BsonType.INT32 -> (a as BsonInt32).value.compareTo((b as BsonInt32).value)
        BsonType.INT64 -> (a as BsonInt64).value.compareTo((b as BsonInt64).value)
        BsonType.DOUBLE -> (a as BsonDouble).value.compareTo((b as BsonDouble).value)
        BsonType.STRING -> (a as BsonString).value.compareTo((b as BsonString).value)
        BsonType.BOOLEAN -> (a as BsonBoolean).value.compareTo((b as BsonBoolean).value)
        BsonType.DATE_TIME -> (a as BsonDateTime).value.compareTo((b as BsonDateTime).value)
        else -> 0
    }

    if (a is BsonNumber && b is BsonNumber) {
        if (a is BsonDouble || b is BsonDouble) return a.doubleValue().compareTo(b.doubleValue())
        return a.longValue().compareTo(b.longValue())
    }

    return a.bsonType.ordinal.compareTo(b.bsonType.ordinal)
}

private fun incrementBsonValue(value: BsonValue, increment: Number): BsonValue {
    return when (value) {
        is BsonInt32 -> BsonInt32(value.value + increment.toInt())
        is BsonInt64 -> BsonInt64(value.value + increment.toLong())
        is BsonDouble -> BsonDouble(value.value + increment.toDouble())
        else -> throw IllegalArgumentException("Cannot increment non-numeric BsonValue: $value")
    }
}

fun Instant.truncatedToMillis(): Instant {
    val millis = this.toEpochMilliseconds()
    return Instant.fromEpochMilliseconds(millis)
}

internal fun toStringId(bsonId: BsonValue?): String? {
    val idValue = bsonId ?: return null
    return when (idValue) {
        is BsonString -> idValue.value
        is BsonInt32 -> idValue.value.toString()
        is BsonInt64 -> idValue.value.toString()
        is BsonDouble -> idValue.value.toString()
        is BsonObjectId -> idValue.value.toHexString()
        else -> null
    }
}