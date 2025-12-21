package dev.helight.kodein.collection

import dev.helight.kodein.incEmbedded
import dev.helight.kodein.putEmbedded
import dev.helight.kodein.unsetEmbedded
import org.bson.BsonDocument
import org.bson.BsonValue

class Update(
    val updates: List<Field>,
    val upsert: Boolean = false
) {
    fun applyUpdates(document: BsonDocument) {
        for (update in updates) {
            if (update is Field.SetOnInsert) continue
            update.applyUpdate(document)
        }
    }

    fun applyUpsert(document: BsonDocument, query: Filter) {
        for (update in query.upsertUpdates) {
            update.applyUpdate(document)
        }
        for (update in updates) {
            update.applyUpdate(document)
        }
    }

    sealed class Field(
        val path: String
    ) {
        class Set(path: String, val value: BsonValue) : Field(path) {
            override fun applyUpdate(document: BsonDocument) {
                document.putEmbedded(path, value)
            }
        }

        class Unset(path: String) : Field(path) {
            override fun applyUpdate(document: BsonDocument) {
                document.unsetEmbedded(path)
            }
        }

        class Inc(path: String, val amount: Number) : Field(path) {
            override fun applyUpdate(document: BsonDocument) {
                document.incEmbedded(path, amount)
            }
        }

        class SetOnInsert(path: String, val value: BsonValue) : Field(path) {
            override fun applyUpdate(document: BsonDocument) {
                document.putEmbedded(path, value)
            }
        }

        abstract fun applyUpdate(document: BsonDocument)
    }

}

