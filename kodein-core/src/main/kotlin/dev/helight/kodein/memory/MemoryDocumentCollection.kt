package dev.helight.kodein.memory

import dev.helight.kodein.KDocument
import dev.helight.kodein.Kodein
import dev.helight.kodein.collection.DocumentCollection
import dev.helight.kodein.collection.Filter
import dev.helight.kodein.collection.FindOptions
import dev.helight.kodein.collection.Update
import dev.helight.kodein.getEmbedded
import dev.helight.kodein.spec.IndexDefinition
import dev.helight.kodein.spec.IndexList
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable
import org.bson.BsonDocument
import org.bson.BsonObjectId
import org.bson.BsonString
import org.bson.BsonValue
import org.bson.types.ObjectId
import java.util.TreeMap

class MemoryDocumentCollection(
    override val kodein: Kodein
) : DocumentCollection {

    private val delegate: MutableList<BsonDocument> = mutableListOf()
    private val mutex = CoroutineReadWriteLock()
    
    // Index structures
    private val indices = mutableMapOf<String, Pair<IndexDefinition, TreeMap<BsonValue, MutableSet<BsonDocument>>>>()
    private val textIndices = mutableMapOf<String, MutableMap<String, MutableSet<BsonDocument>>>()
    private var indexList: IndexList? = null
    private var optimizer: QueryOptimizer? = null

    /**
     * Initialize indexes from the spec system
     */
    fun setIndices(indexList: IndexList) {
        this.indexList = indexList
        
        // Create index structures
        for (indexDef in indexList.indices) {
            indices[indexDef.path] = Pair(
                indexDef,
                TreeMap { v1, v2 -> 
                    dev.helight.kodein.compareBsonValues(v1, v2)
                }
            )
        }
        
        // Initialize text indices
        for (textField in indexList.textIndices) {
            textIndices[textField] = mutableMapOf()
        }
        
        // Rebuild optimizer
        rebuildOptimizer()
    }

    private fun rebuildOptimizer() {
        val indexMap = indices.mapValues { (_, pair) -> 
            Pair(pair.first, pair.second as Any)
        }
        optimizer = QueryOptimizer(indexMap, textIndices.keys, delegate.size)
    }

    private fun addToIndex(document: BsonDocument) {
        // Add to regular indices
        for ((path, indexPair) in indices) {
            val value = document.getEmbedded(path)
            if (value != null) {
                val indexMap = indexPair.second
                indexMap.computeIfAbsent(value) { mutableSetOf() }.add(document)
            }
        }
        
        // Add to text indices
        for ((path, textIndex) in textIndices) {
            val value = document.getEmbedded(path)
            if (value is BsonString) {
                val tokens = tokenize(value.value)
                for (token in tokens) {
                    textIndex.computeIfAbsent(token) { mutableSetOf() }.add(document)
                }
            }
        }
    }

    private fun removeFromIndex(document: BsonDocument) {
        // Remove from regular indices
        for ((path, indexPair) in indices) {
            val value = document.getEmbedded(path)
            if (value != null) {
                val indexMap = indexPair.second
                indexMap[value]?.remove(document)
                if (indexMap[value]?.isEmpty() == true) {
                    indexMap.remove(value)
                }
            }
        }
        
        // Remove from text indices
        for ((path, textIndex) in textIndices) {
            val value = document.getEmbedded(path)
            if (value is BsonString) {
                val tokens = tokenize(value.value)
                for (token in tokens) {
                    textIndex[token]?.remove(document)
                    if (textIndex[token]?.isEmpty() == true) {
                        textIndex.remove(token)
                    }
                }
            }
        }
    }

    private fun removeFromIndexByValue(document: BsonDocument, oldValues: Map<String, BsonValue?>) {
        // Remove from regular indices using old values
        for ((path, indexPair) in indices) {
            val oldValue = oldValues[path]
            if (oldValue != null) {
                val indexMap = indexPair.second
                indexMap[oldValue]?.remove(document)
                if (indexMap[oldValue]?.isEmpty() == true) {
                    indexMap.remove(oldValue)
                }
            }
        }
        
        // Remove from text indices using old values
        for ((path, textIndex) in textIndices) {
            val oldValue = oldValues[path]
            if (oldValue is BsonString) {
                val tokens = tokenize(oldValue.value)
                for (token in tokens) {
                    textIndex[token]?.remove(document)
                    if (textIndex[token]?.isEmpty() == true) {
                        textIndex.remove(token)
                    }
                }
            }
        }
    }

    private fun updateIndex(document: BsonDocument, oldValues: Map<String, BsonValue?>) {
        // First remove old values from indices
        removeFromIndexByValue(document, oldValues)
        // Then add new values to indices
        addToIndex(document)
    }

    private fun tokenize(text: String): Set<String> {
        return text.lowercase()
            .split(Regex("\\W+"))
            .filter { it.isNotBlank() }
            .toSet()
    }

    private fun executeQuery(filter: Filter?): Sequence<BsonDocument> {
        if (filter == null) {
            return delegate.asSequence()
        }

        val plan = optimizer?.optimize(filter) ?: QueryPlan.FullScan(filter, delegate.size)
        
        return when (plan) {
            is QueryPlan.FullScan -> {
                delegate.asSequence().filter { filter.eval(it) }
            }
            is QueryPlan.IndexScan -> {
                val candidates = getIndexCandidates(plan.indexedFilter)
                // Always evaluate the indexed filter on candidates to ensure they still match
                val filtered = candidates.filter { plan.indexedFilter.eval(it) }
                if (plan.remainingFilter != null) {
                    filtered.filter { plan.remainingFilter.eval(it) }
                } else {
                    filtered
                }
            }
            is QueryPlan.TextIndexScan -> {
                val candidates = getTextCandidates(plan.textFilter)
                // Create a filter with indexed fields specified for proper evaluation
                val textFilterWithFields = Filter.Text(plan.textFilter.searchTerm, plan.indexedFields)
                // Always evaluate the text filter on candidates to ensure they still match
                val filtered = candidates.filter { textFilterWithFields.eval(it) }
                if (plan.remainingFilter != null) {
                    filtered.filter { plan.remainingFilter.eval(it) }
                } else {
                    filtered
                }
            }
        }
    }

    private fun getIndexCandidates(filter: Filter.Field): Sequence<BsonDocument> {
        val indexPair = indices[filter.path] ?: return emptySequence()
        val indexMap = indexPair.second
        
        return when (filter) {
            is Filter.Field.Eq -> {
                indexMap[filter.value]?.asSequence() ?: emptySequence()
            }
            is Filter.Field.In -> {
                filter.value.flatMap { value ->
                    indexMap[value]?.toList() ?: emptyList()
                }.asSequence()
            }
            is Filter.Field.Comp -> {
                val comparator = indexMap.comparator()
                when (filter.type) {
                    Filter.CompType.GT -> indexMap.tailMap(filter.value, false)
                    Filter.CompType.GTE -> indexMap.tailMap(filter.value, true)
                    Filter.CompType.LT -> indexMap.headMap(filter.value, false)
                    Filter.CompType.LTE -> indexMap.headMap(filter.value, true)
                }.values.flatMap { it }.asSequence()
            }
            else -> emptySequence()
        }
    }

    private fun getTextCandidates(filter: Filter.Text): Sequence<BsonDocument> {
        val searchTokens = tokenize(filter.searchTerm.value)
        
        if (searchTokens.isEmpty()) return emptySequence()
        
        // Collect documents from all text indices that match any search token
        val candidateSet = mutableSetOf<BsonDocument>()
        for ((_, textIndex) in textIndices) {
            for (token in searchTokens) {
                textIndex[token]?.let { candidateSet.addAll(it) }
            }
        }
        
        return candidateSet.asSequence()
    }

    /**
     * Explain the query plan for a filter
     * This method is NOT part of the base DocumentCollection interface
     */
    fun explain(filter: Filter?): QueryExplanation {
        val plan = optimizer?.optimize(filter) ?: QueryPlan.FullScan(filter, delegate.size)
        val opt = optimizer ?: QueryOptimizer(emptyMap(), emptySet(), delegate.size)
        return opt.explain(plan)
    }


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
        // Clear indices
        for ((_, indexPair) in indices) {
            indexPair.second.clear()
        }
        for ((_, textIndex) in textIndices) {
            textIndex.clear()
        }
        // Load documents and rebuild indices
        for (doc in state.documents) {
            delegate.add(doc)
            addToIndex(doc)
        }
        rebuildOptimizer()
    }

    private fun ensureId(document: BsonDocument) {
        val idValue = document["_id"]
        if (idValue == null) {
            val newId = ObjectId.get()
            document["_id"] = BsonObjectId(newId)
        }
    }

    private fun shallowCopy(document: BsonDocument): BsonDocument {
        val copy = BsonDocument()
        for ((key, value) in document) {
            copy[key] = value
        }
        return copy
    }

    override suspend fun insert(document: BsonDocument): Boolean = mutex.write {
        ensureId(document)
        delegate.add(document)
        addToIndex(document)
        rebuildOptimizer()
        return true
    }

    override suspend fun update(
        filter: Filter,
        update: Update
    ): Int = mutex.write {
        val candidates = executeQuery(filter).toList()
        for (current in candidates) {
            // Capture old values before mutation
            val oldValues = mutableMapOf<String, BsonValue?>()
            for ((path, _) in indices) {
                oldValues[path] = current.getEmbedded(path)
            }
            for (path in textIndices.keys) {
                oldValues[path] = current.getEmbedded(path)
            }
            
            update.applyUpdates(current)
            updateIndex(current, oldValues)
        }
        if (update.upsert && candidates.isEmpty()) {
            val newDocument = BsonDocument()
            update.applyUpsert(newDocument, filter)
            ensureId(newDocument)
            delegate.add(newDocument)
            addToIndex(newDocument)
            rebuildOptimizer()
            return 1
        }
        return candidates.size
    }

    override suspend fun updateOne(
        filter: Filter,
        update: Update
    ): Boolean = mutex.write {
        val entry = executeQuery(filter).firstOrNull()
        if (entry != null) {
            // Capture old values before mutation
            val oldValues = mutableMapOf<String, BsonValue?>()
            for ((path, _) in indices) {
                oldValues[path] = entry.getEmbedded(path)
            }
            for (path in textIndices.keys) {
                oldValues[path] = entry.getEmbedded(path)
            }
            
            update.applyUpdates(entry)
            updateIndex(entry, oldValues)
            return true
        } else if (update.upsert) {
            val newDocument = BsonDocument()
            update.applyUpsert(newDocument, filter)
            ensureId(newDocument)
            delegate.add(newDocument)
            addToIndex(newDocument)
            rebuildOptimizer()
            return true
        } else {
            return false
        }
    }

    override suspend fun updateOneReturning(
        filter: Filter,
        update: Update
    ): KDocument? = mutex.write {
        val entry = executeQuery(filter).firstOrNull()
        if (entry != null) {
            // Capture old values before mutation
            val oldValues = mutableMapOf<String, BsonValue?>()
            for ((path, _) in indices) {
                oldValues[path] = entry.getEmbedded(path)
            }
            for (path in textIndices.keys) {
                oldValues[path] = entry.getEmbedded(path)
            }
            
            update.applyUpdates(entry)
            updateIndex(entry, oldValues)
            return kodein.introspect(entry)
        } else if (update.upsert) {
            val newDocument = BsonDocument()
            update.applyUpsert(newDocument, filter)
            ensureId(newDocument)
            delegate.add(newDocument)
            addToIndex(newDocument)
            rebuildOptimizer()
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
        val entry = executeQuery(filter).firstOrNull()
        if (entry != null) {
            val id = entry["_id"]
            removeFromIndex(entry)
            entry.clear()
            entry.putAll(document)
            entry["_id"] = id
            addToIndex(entry)
            return true
        } else if (upsert) {
            for (update in filter.upsertUpdates) {
                if (update.path != "_id") continue
                update.applyUpdate(document)
            }
            ensureId(document)
            delegate.add(document)
            addToIndex(document)
            rebuildOptimizer()
            return true
        } else {
            return false
        }
    }

    override suspend fun delete(filter: Filter): Int = mutex.write {
        val toDelete = executeQuery(filter).toList()
        for (doc in toDelete) {
            removeFromIndex(doc)
            delegate.remove(doc)
        }
        if (toDelete.isNotEmpty()) {
            rebuildOptimizer()
        }
        return toDelete.size
    }

    override suspend fun deleteOne(filter: Filter): Boolean = mutex.write {
        val entry = executeQuery(filter).firstOrNull()
        if (entry != null) {
            removeFromIndex(entry)
            delegate.remove(entry)
            rebuildOptimizer()
            return true
        } else {
            return false
        }
    }

    override suspend fun count(filter: Filter?): Long = mutex.read {
        when (filter) {
            null -> delegate.size.toLong()
            else -> executeQuery(filter).count().toLong()
        }
    }

    override suspend fun find(
        filter: Filter?,
        options: FindOptions
    ): Flow<KDocument> = mutex.read {
        val seq = executeQuery(filter)
        return MemoryFinder.sortCursorAndProject(options, seq).map {
            KDocument(kodein, it)
        }.toList().asFlow()
    }

    override suspend fun findOne(
        filter: Filter,
        options: FindOptions
    ): KDocument? = mutex.read {
        val seq = executeQuery(filter)
        return MemoryFinder.sortCursorAndProject(options, seq).map {
            KDocument(kodein, it)
        }.firstOrNull()
    }
}