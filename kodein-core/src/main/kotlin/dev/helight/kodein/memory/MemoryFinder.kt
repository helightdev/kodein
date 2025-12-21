package dev.helight.kodein.memory

import dev.helight.kodein.collection.Filter
import dev.helight.kodein.collection.FindOptions
import dev.helight.kodein.compareBsonValues
import dev.helight.kodein.getEmbedded
import dev.helight.kodein.putEmbedded
import org.bson.BsonDocument

object MemoryFinder {

    fun project(document: BsonDocument, fields: Set<String>): BsonDocument {
        val projected = BsonDocument()
        for (field in fields) {
            val value = document.getEmbedded(field)
            if (value != null) projected.putEmbedded(field, value)
        }
        return projected
    }

    fun find(
        filter: Filter?,
        options: FindOptions,
        seq: Sequence<BsonDocument>
    ): Sequence<BsonDocument> {
        val filtered = filter(filter, seq)
        return sortCursorAndProject(options, filtered)
    }

    fun filter(filter: Filter?, seq: Sequence<BsonDocument>): Sequence<BsonDocument> {
        if (filter == null) return seq
        return seq.filter { doc -> filter.eval(doc) }
    }

    fun <T> sortCursorMapped(options: FindOptions, seq: Sequence<T>, mapper: (T) -> BsonDocument): Sequence<T> {
        var result = seq
        if (options.sort.isNotEmpty()) {
            result = result.sortedWith { doc1, doc2 ->
                for (s in options.sort) {
                    val val1 = mapper(doc1).getEmbedded(s.path)
                    val val2 = mapper(doc2).getEmbedded(s.path)
                    val cmp = compareBsonValues(val1, val2)
                    if (cmp != 0) {
                        return@sortedWith if (s.ascending) cmp else -cmp
                    }
                }
                0
            }
        }
        return result.drop(options.skip).take(options.limit)
    }

    fun sortCursorAndProject(options: FindOptions, seq: Sequence<BsonDocument>): Sequence<BsonDocument> {
        var result = seq
        if (options.sort.isNotEmpty()) {
            result = result.sortedWith { doc1, doc2 ->
                for (s in options.sort) {
                    val val1 = doc1.getEmbedded(s.path)
                    val val2 = doc2.getEmbedded(s.path)
                    val cmp = compareBsonValues(val1, val2)
                    if (cmp != 0) {
                        return@sortedWith if (s.ascending) cmp else -cmp
                    }
                }
                0
            }
        }

        result = result.drop(options.skip).take(
            when (options.limit >= Int.MAX_VALUE.toLong()) {
                true -> Int.MAX_VALUE
                false -> options.limit
            }
        )
        if (options.fields.isNotEmpty()) result = result.map { project(it, options.fields) }
        return result
    }
}