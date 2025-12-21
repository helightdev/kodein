package dev.helight.kodein.mongo

import com.mongodb.client.model.Filters
import com.mongodb.client.model.Sorts
import com.mongodb.client.model.Updates
import dev.helight.kodein.collection.Filter
import dev.helight.kodein.collection.FindOptions
import dev.helight.kodein.collection.Update
import org.bson.BsonDocument
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

    fun convertLogicFilter(filter: Filter): Bson {
        return when (filter) {
            is Filter.Field -> convertFieldFilter(filter)
            is Filter.And -> when {
                filter.filters.isEmpty() -> BsonDocument()
                else -> Filters.and(filter.filters.map(::convertLogicFilter))
            }
            is Filter.Or -> Filters.or(filter.filters.map(::convertLogicFilter))
            is Filter.Not -> Filters.not(convertLogicFilter(filter.filter))
        }
    }

    fun convertFieldFilter(fieldFilter: Filter.Field): Bson {
        return when (fieldFilter) {
            is Filter.Field.Eq -> Filters.eq(fieldFilter.path, fieldFilter.value)
            is Filter.Field.Ne -> Filters.ne(fieldFilter.path, fieldFilter.value)
            is Filter.Field.In -> Filters.`in`(fieldFilter.path, fieldFilter.value)
            is Filter.Field.Nin -> Filters.nin(fieldFilter.path, fieldFilter.value)
            is Filter.Field.Comp -> when (fieldFilter.type) {
                Filter.CompType.GT -> Filters.gt(fieldFilter.path, fieldFilter.value)
                Filter.CompType.GTE -> Filters.gte(fieldFilter.path, fieldFilter.value)
                Filter.CompType.LT -> Filters.lt(fieldFilter.path, fieldFilter.value)
                Filter.CompType.LTE -> Filters.lte(fieldFilter.path, fieldFilter.value)
            }
        }
    }

    fun convertUpdates(update: Update): Bson = Updates.combine(update.updates.map { fu ->
        when (fu) {
            is Update.Field.Set -> Updates.set(fu.path, fu.value)
            is Update.Field.Unset -> Updates.unset(fu.path)
            is Update.Field.Inc -> Updates.inc(fu.path, fu.amount)
            is Update.Field.SetOnInsert -> Updates.setOnInsert(fu.path, fu.value)
        }
    })

}

