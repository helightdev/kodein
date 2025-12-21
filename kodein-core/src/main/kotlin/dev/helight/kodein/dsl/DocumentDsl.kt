package dev.helight.kodein.dsl

import dev.helight.kodein.BsonMarshaller
import org.bson.*
import kotlin.time.Instant

@DslMarker
annotation class DocumentDsl

fun documentOf(): BsonDocument = BsonDocument()
fun documentOf(vararg pairs: Pair<String, BsonValue>): BsonDocument {
    val doc = BsonDocument()
    for ((key, value) in pairs) {
        doc[key] = value
    }
    return doc
}
fun documentOf(map: Map<String, BsonValue>): BsonDocument {
    val doc = BsonDocument()
    for ((key, value) in map) {
        doc[key] = value
    }
    return doc
}

fun buildDocument(builderAction: BsonDocumentBuilder.() -> Unit): BsonDocument {
    val doc = BsonDocumentBuilder()
    doc.builderAction()
    return doc.bson
}

fun BsonDocument.modify(builderAction: BsonDocumentBuilder.() -> Unit): BsonDocument {
    val docBuilder = BsonDocumentBuilder(this)
    docBuilder.builderAction()
    return docBuilder.build()
}

@DocumentDsl
class BsonDocumentBuilder(
    val bson: BsonDocument = BsonDocument()
) {
    infix fun String.put(value: BsonValue) {
        bson[this] = value
    }

    infix fun String.put(value: String) {
        bson[this] = BsonString(value)
    }

    infix fun String.put(value: Int) {
        bson[this] = BsonInt32(value)
    }

    infix fun String.put(value: Long) {
        bson[this] = BsonInt64(value)
    }

    infix fun String.put(value: Boolean) {
        bson[this] = BsonBoolean(value)
    }

    infix fun String.put(value: Double) {
        bson[this] = BsonDouble(value)
    }

    infix fun String.put(value: ByteArray) {
        bson[this] = BsonBinary(value)
    }

    infix fun String.put(value: Instant) {
        bson[this] = BsonDateTime(value.toEpochMilliseconds())
    }

    infix fun String.put(value: Nothing?) {
        bson[this] = BsonNull.VALUE
    }

    infix fun String.put(any: Any?) {
        bson[this] = BsonMarshaller.marshal(any)
    }

    infix fun String.put(builderAction: BsonDocumentBuilder.() -> Unit) {
        val docBuilder = BsonDocumentBuilder()
        docBuilder.builderAction()
        bson[this] = docBuilder.build()
    }

    infix fun String.putArray(builderAction: BsonArrayBuilder.() -> Unit) {
        val arrayBuilder = BsonArrayBuilder()
        arrayBuilder.builderAction()
        bson[this] = arrayBuilder.build()
    }

    fun build(): BsonDocument = bson
}

@DocumentDsl
class BsonArrayBuilder(
    val bsonArray: BsonArray = BsonArray()
) {
    fun add(value: BsonValue) {
        bsonArray.add(value)
    }

    fun add(value: String) {
        bsonArray.add(BsonString(value))
    }

    fun add(value: Int) {
        bsonArray.add(BsonInt32(value))
    }

    fun add(value: Long) {
        bsonArray.add(BsonInt64(value))
    }

    fun add(value: Boolean) {
        bsonArray.add(BsonBoolean(value))
    }

    fun add(value: Double) {
        bsonArray.add(BsonDouble(value))
    }

    fun add(value: ByteArray) {
        bsonArray.add(BsonBinary(value))
    }

    fun add(value: Instant) {
        bsonArray.add(BsonDateTime(value.toEpochMilliseconds()))
    }

    fun add(value: Nothing?) {
        bsonArray.add(BsonNull.VALUE)
    }

    fun addArray(builderAction: BsonArrayBuilder.() -> Unit) {
        val arrayBuilder = BsonArrayBuilder()
        arrayBuilder.builderAction()
        bsonArray.add(arrayBuilder.build())
    }

    fun addDocument(builderAction: BsonDocumentBuilder.() -> Unit) {
        val docBuilder = BsonDocumentBuilder()
        docBuilder.builderAction()
        bsonArray.add(docBuilder.bson)
    }

    fun build(): BsonArray = bsonArray
}
