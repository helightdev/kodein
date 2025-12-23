package dev.helight.kodein.memory

import dev.helight.kodein.KDocument
import dev.helight.kodein.Kodein
import dev.helight.kodein.collection.DocumentCollection
import dev.helight.kodein.collection.Filter
import dev.helight.kodein.collection.FindOptions
import dev.helight.kodein.collection.Update
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable
import org.bson.BsonDocument
import org.bson.BsonObjectId
import org.bson.types.ObjectId

class MemoryDocumentCollection(
    override val kodein: Kodein
) : DocumentCollection {

    private val delegate: MutableList<BsonDocument> = mutableListOf()
    private val mutex = CoroutineReadWriteLock()

    @Serializable
    private class SerializedCollection(
        val documents: List<@Contextual BsonDocument>
    )

    suspend fun dumpBytes(): ByteArray = mutex.read {
        val state = SerializedCollection(documents = delegate.toList())
        return kodein.encodeBinary(state)
    }

    suspend fun loadBytes(bytes: ByteArray) = mutex.write {
        val state = kodein.decodeBinary<SerializedCollection>(bytes)
        delegate.clear()
        for (doc in state.documents) {
            delegate.add(doc)
        }
    }

    private fun ensureId(document: BsonDocument) {
        val idValue = document["_id"]
        if (idValue == null) {
            val newId = ObjectId.get()
            document["_id"] = BsonObjectId(newId)
        }
    }

    override suspend fun insert(document: BsonDocument): Boolean = mutex.write {
        ensureId(document)
        delegate.add(document)
        return true
    }

    override suspend fun update(
        filter: Filter,
        update: Update
    ): Int = mutex.write {
        val candidates = delegate.filter { filter.eval(it) }.toList()
        for (current in candidates) update.applyUpdates(current)
        if (update.upsert && candidates.isEmpty()) {
            val newDocument = BsonDocument()
            update.applyUpsert(newDocument, filter)
            ensureId(newDocument)
            delegate.add(newDocument)
            return 1
        }
        return candidates.size
    }

    override suspend fun updateOne(
        filter: Filter,
        update: Update
    ): Boolean = mutex.write {
        val entry = delegate.firstOrNull { filter.eval(it) }
        if (entry != null) {
            update.applyUpdates(entry)
            return true
        } else if (update.upsert) {
            val newDocument = BsonDocument()
            update.applyUpsert(newDocument, filter)
            ensureId(newDocument)
            delegate.add(newDocument)
            return true
        } else {
            return false
        }
    }

    override suspend fun updateOneReturning(
        filter: Filter,
        update: Update
    ): KDocument? = mutex.write {
        val entry = delegate.firstOrNull { filter.eval(it) }
        if (entry != null) {
            update.applyUpdates(entry)
            return kodein.introspect(entry)
        } else if (update.upsert) {
            val newDocument = BsonDocument()
            update.applyUpsert(newDocument, filter)
            ensureId(newDocument)
            delegate.add(newDocument)
            return kodein.introspect(newDocument)
        } else {
            return null
        }
    }

    override suspend fun replace(
        filter: Filter,
        document: BsonDocument,
        upsert: Boolean
    ): Boolean = mutex.write {
        val entry = delegate.firstOrNull { filter.eval(it) }
        if (entry != null) {
            val id = entry["_id"]
            entry.clear()
            entry.putAll(document)
            entry["_id"] = id
            return true
        } else if (upsert) {
            for (update in filter.upsertUpdates) {
                if (update.path != "_id") continue
                update.applyUpdate(document)
            }
            ensureId(document)
            delegate.add(document)
            return true
        } else {
            return false
        }
    }

    override suspend fun delete(filter: Filter): Int = mutex.write {
        var counter = 0
        delegate.removeAll {
            val matched = filter.eval(it)
            if (matched) counter++
            matched
        }
        return counter
    }

    override suspend fun deleteOne(filter: Filter): Boolean = mutex.write {
        val entry = delegate.firstOrNull { filter.eval(it) }
        if (entry != null) {
            delegate.remove(entry)
            return true
        } else {
            return false
        }
    }

    override suspend fun count(filter: Filter?): Long = mutex.read {
        when (filter) {
            null -> delegate.size.toLong()
            else -> delegate.count { filter.eval(it) }.toLong()
        }
    }

    override suspend fun find(
        filter: Filter?,
        options: FindOptions
    ): Flow<KDocument> = mutex.read {
        var seq = delegate.asSequence()
        if (filter != null) seq = seq.filter { filter.eval(it) }
        return MemoryFinder.sortCursorAndProject(options, seq).map {
            KDocument(kodein, it)
        }.asFlow()
    }

    override suspend fun findOne(
        filter: Filter,
        options: FindOptions
    ): KDocument? = mutex.read {
        return find(filter, options.copy(limit = 1)).firstOrNull()
    }
}