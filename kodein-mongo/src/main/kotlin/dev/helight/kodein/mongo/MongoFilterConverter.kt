package dev.helight.kodein.mongo

import com.mongodb.client.model.Filters
import com.mongodb.client.model.Sorts
import com.mongodb.client.model.Updates
import dev.helight.kodein.collection.Filter
import dev.helight.kodein.collection.FindOptions
import dev.helight.kodein.collection.Update
import dev.helight.kodein.dsl.buildDocument
import dev.helight.kodein.dsl.documentOf
import org.bson.BsonArray
import org.bson.BsonDocument
import org.bson.BsonNumber
import org.bson.BsonType
import org.bson.Document
import org.bson.conversions.Bson

object MongoFilterConverter {

    fun projection(fields: Set<String>): Bson {
        val root = Document()
        for (path in fields) {
            val parts = path.split(".")
            var current: Document = root

            for ((index, part) in parts.withIndex()) {
                if (index == parts.lastIndex) {
                    current[part] = 1
                } else {
                    val nested = current[part] as? Document
                        ?: Document().also { current[part] = it }
                    current = nested
                }
            }
        }
        return root
    }

    fun convertSort(sort: List<FindOptions.Sort>): Bson = Sorts.orderBy(sort.map {
        if (it.ascending) Sorts.ascending(it.path) else Sorts.descending(it.path)
    })

    fun convertUpdates(update: Update): Bson = Updates.combine(update.updates.map { fu ->
        when (fu) {
            is Update.Field.Set -> Updates.set(fu.path, fu.value)
            is Update.Field.Unset -> Updates.unset(fu.path)
            is Update.Field.Inc -> Updates.inc(fu.path, fu.amount)
            is Update.Field.SetOnInsert -> Updates.setOnInsert(fu.path, fu.value)
        }
    })

    fun convert(filter: Filter, relaxed: Boolean = false): Bson {
        val builder = MongoQueryBuilder()
        convert(filter, relaxed, builder)
        return builder.buildRootAnd()
    }


    fun convert(filter: Filter, relaxed: Boolean, builder: MongoQueryBuilder) {
        when (filter) {
            is Filter.Native -> builder.addFilter(filter.value as Bson)
            is Filter.And -> builder.addAnd(MongoQueryBuilder().apply {
                filter.filters.forEach { convert(it, relaxed, this) }
            })

            is Filter.Or -> builder.addOr(MongoQueryBuilder().apply {
                filter.filters.forEach { convert(it, relaxed, this) }
            })

            is Filter.Not -> builder.addNot(MongoQueryBuilder().apply { convert(filter.filter, relaxed, this) })

            is Filter.Field.Eq -> when {
                relaxed -> builder.addFilter(Filters.eq(filter.path, filter.value))
                filter.value is BsonArray -> builder.addFilter(Filters.eq(filter.path, filter.value))
                else -> builder.inlineAnd(
                    Filters.eq(filter.path, filter.value),
                    Filters.not(Filters.type(filter.path, BsonType.ARRAY))
                )
            }
            is Filter.Field.Ne -> when {
                relaxed -> builder.addFilter(Filters.ne(filter.path, filter.value))
                filter.value is BsonArray -> builder.addFilter(Filters.ne(filter.path, filter.value))
                else -> builder.addFilter(Filters.or(
                    Filters.ne(filter.path, filter.value),
                    Filters.type(filter.path, BsonType.ARRAY)
                ))
            }

            is Filter.Field.In -> when {
                relaxed -> builder.addFilter(Filters.`in`(filter.path, filter.value))
                filter.value.isEmpty() -> error("in filter cannot have an empty array")
                filter.value.first() is BsonArray -> error("in filter cannot have multi-dimensional arrays")
                else -> builder.inlineAnd(
                    Filters.`in`(filter.path, filter.value),
                    Filters.not(Filters.type(filter.path, BsonType.ARRAY))
                )
            }

            is Filter.Field.Nin -> when {
                relaxed -> builder.addFilter(Filters.nin(filter.path, filter.value))
                filter.value.isEmpty() -> error("notIn filter cannot have an empty array")
                filter.value.first() is BsonArray -> error("notIn filter cannot have multi-dimensional arrays")
                else -> builder.addFilter(Filters.or(
                    Filters.nin(filter.path, filter.value),
                    Filters.type(filter.path, BsonType.ARRAY)
                ))
            }

            is Filter.Field.Comp -> when(relaxed) {
                true -> builder.addFilter(when (filter.type) {
                    Filter.CompType.GT -> Filters.gt(filter.path, filter.value)
                    Filter.CompType.GTE -> Filters.gte(filter.path, filter.value)
                    Filter.CompType.LT -> Filters.lt(filter.path, filter.value)
                    Filter.CompType.LTE -> Filters.lte(filter.path, filter.value)
                })
                else -> builder.inlineAnd(
                    when (filter.type) {
                        Filter.CompType.GT -> Filters.gt(filter.path, filter.value)
                        Filter.CompType.GTE -> Filters.gte(filter.path, filter.value)
                        Filter.CompType.LT -> Filters.lt(filter.path, filter.value)
                        Filter.CompType.LTE -> Filters.lte(filter.path, filter.value)
                    },
                    Filters.not(Filters.type(filter.path, BsonType.ARRAY))
                )
            }

            is Filter.Field.ArrCont -> when {
                relaxed -> builder.addFilter(Filters.eq(filter.path, filter.value))
                else -> builder.inlineAnd(
                    Filters.eq(filter.path, filter.value),
                    Filters.type(filter.path, BsonType.ARRAY)
                )
            }

            is Filter.Field.ArrNotCont -> when {
                relaxed -> builder.addFilter(Filters.ne(filter.path, filter.value))
                else -> builder.addFilter(Filters.or(
                    Filters.ne(filter.path, filter.value),
                    Filters.not(Filters.type(filter.path, BsonType.ARRAY))
                ))
            }
            is Filter.Field.ArrSize -> builder.addFilter(Filters.size(filter.path, filter.value.intValue()))
            is Filter.Field.ArrComp -> when(filter.type) {
                Filter.ArrayCompType.ANY -> when(relaxed) {
                    true -> builder.addFilter(Filters.`in`(filter.path, filter.value))
                    else -> builder.inlineAnd(
                        Filters.`in`(filter.path, filter.value),
                        Filters.type(filter.path, BsonType.ARRAY)
                    )
                }
                Filter.ArrayCompType.NONE -> when(relaxed) {
                    true -> builder.addFilter(Filters.nin(filter.path, filter.value))
                    else -> builder.addFilter(Filters.or(
                        Filters.nin(filter.path, filter.value),
                        Filters.not(Filters.type(filter.path, BsonType.ARRAY))
                    ))
                }
                Filter.ArrayCompType.ALL -> builder.addFilter(
                    Filters.all(filter.path, filter.value)
                )
                Filter.ArrayCompType.SET -> builder.inlineAnd(
                    Filters.all(filter.path, filter.value),
                    Filters.size(filter.path, filter.value.size)
                )
            }

            is Filter.Field.Regex -> when {
                relaxed -> builder.addFilter(documentOf(filter.path to filter.value))
                else -> builder.inlineAnd(
                    documentOf(filter.path to filter.value),
                    Filters.not(Filters.type(filter.path, BsonType.ARRAY))
                )
            }

            else -> error("Unsupported filter type: $filter")
        }
    }
}

class MongoQueryBuilder {
    private val buffer: MutableList<Bson> = mutableListOf()

    fun addOr(query: MongoQueryBuilder) {
        buffer.add(query.buildAsOr())
    }

    fun addAnd(query: MongoQueryBuilder) {
        query.buildAsAnd()?.let { buffer.add(it) }
    }

    fun inlineAnd(vararg filters: Bson) {
        addAnd(MongoQueryBuilder().apply {
            for (filter in filters) { addFilter(filter) }
        })
    }

    fun addNot(query: MongoQueryBuilder) {
        buffer.add(query.buildAsNot())
    }

    fun addFilter(filter: Bson) {
        buffer.add(filter)
    }

    fun buildRootAnd(): Bson {
        val a = when (buffer.size) {
            0 -> documentOf()
            1 -> buffer[0]
            else -> Filters.and(buffer)
        }.toBsonDocument()

        // Convert root level NOTs to NORs
        if (a.containsKey($$"$not") && a.size == 1) {
            val notContent = a[$$"$not"]!!
            val newDoc = buildDocument {
                $$"$nor" put listOf(notContent)
            }
            return newDoc
        }

        return a

    }

    private fun buildAsOr(): Bson {
        return when (buffer.size) {
            0 -> throw IllegalStateException("Empty or will match nothing")
            1 -> buffer[0]
            else -> Filters.or(buffer)
        }
    }

    private fun buildAsAnd(): Bson? {
        return when (buffer.size) {
            0 -> null
            1 -> buffer[0]
            else -> Filters.and(buffer)
        }
    }

    private fun buildAsNot(): Bson {
        return when (buffer.size) {
            0 -> throw IllegalStateException("Empty not will match everything")
            1 -> Filters.not(buffer[0])
            else -> Filters.not(Filters.and(buffer))
        }
    }
}
