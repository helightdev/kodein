package dev.helight.kodein.spec

import dev.helight.kodein.KDocument
import dev.helight.kodein.toStringId
import org.bson.BsonObjectId
import org.bson.BsonString
import org.bson.BsonValue
import org.bson.types.ObjectId

abstract class BaseDocument {
    open var bsonId: BsonValue? = null
    var document: KDocument? = null

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
        get() = toStringId(bsonId)
        set(value) = when (value) {
            null -> bsonId = null
            else -> bsonId = BsonString(value)
        }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is BaseDocument) return false
        if (bsonId != other.bsonId) return false
        return true
    }

    override fun hashCode(): Int {
        return bsonId?.hashCode() ?: 0
    }

    override fun toString(): String {
        return "BaseDocument(id=$bsonId)"
    }
}