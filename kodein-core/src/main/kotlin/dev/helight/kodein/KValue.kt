package dev.helight.kodein

import org.bson.BsonValue
import org.bson.types.ObjectId

class KValue<T>(
    val kodein: Kodein,
    val document: KDocument,
    val value: T
) {
    val id: ObjectId?
        get() = document.id
    val bsonId: BsonValue?
        get() = document.bsonId
    val stringId: String?
        get() = document.stringId

    fun get(): T = value
    operator fun component1(): T = value
    operator fun component2(): ObjectId? = id
}