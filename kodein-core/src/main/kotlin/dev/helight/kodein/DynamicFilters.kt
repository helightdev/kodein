package dev.helight.kodein

import dev.helight.kodein.BsonMarshaller.toBson
import dev.helight.kodein.collection.Filter
import dev.helight.kodein.spec.FieldNameProducer
import kotlinx.serialization.json.Json
import org.bson.BsonArray

class DynamicFilters(
    val permittedFields: Set<String>? = null
) {
    companion object {
        val default = DynamicFilters()

        fun forSpecs(vararg spec: FieldNameProducer): DynamicFilters {
            val permissible = spec.flatMap { it.getFieldsNames() }.toSet()
            return DynamicFilters(permissible)
        }
    }

    fun parseAll(strings: List<String>): Filter {
        val filters = strings.map { parseSingle(it) }
        return if (filters.size == 1) filters[0] else Filter.And(filters)
    }

    fun parseSingle(str: String): Filter {
        val it = str.split(":")
        if (it.size !in 2..3) throw IllegalArgumentException("Invalid filter format: $str")
        val key = it[0].trim()
        if (permittedFields != null && key !in permittedFields) {
            throw IllegalArgumentException("Field '$key' is not permitted in filters")
        }
        val operator = if (it.size == 3) it[1].trim() else "eq"
        val value = Json.parseToJsonElement(it.last()).toBson()

        return when (operator) {
            "eq" -> Filter.Field.Eq(key, value)
            "ne" -> Filter.Field.Ne(key, value)
            "in" -> Filter.Field.In(
                key,
                requireNotNull(value as? BsonArray) { "Value for 'in' operator must be a BsonArray" })

            "nin" -> Filter.Field.Nin(
                key,
                requireNotNull(value as? BsonArray) { "Value for 'nin' operator must be a BsonArray" })

            "gt" -> Filter.Field.Comp(key, value, Filter.CompType.GT)
            "lt" -> Filter.Field.Comp(key, value, Filter.CompType.LT)
            "gte" -> Filter.Field.Comp(key, value, Filter.CompType.GTE)
            "lte" -> Filter.Field.Comp(key, value, Filter.CompType.LTE)
            "ain" -> Filter.Field.ArrCont(key, value)
            "anin" -> Filter.Field.ArrNotCont(key, value)
            "aany" -> Filter.Field.ArrComp(
                key,
                requireNotNull(value as? BsonArray) { "Value for 'aany' operator must be a BsonArray" },
                Filter.ArrayCompType.ANY
            )
            "aall" -> Filter.Field.ArrComp(
                key,
                requireNotNull(value as? BsonArray) { "Value for 'aall' operator must be a BsonArray" },
                Filter.ArrayCompType.ALL
            )
            "aset" -> Filter.Field.ArrComp(
                key,
                requireNotNull(value as? BsonArray) { "Value for 'aset' operator must be a BsonArray" },
                Filter.ArrayCompType.SET
            )
            "anone" -> Filter.Field.ArrComp(
                key,
                requireNotNull(value as? BsonArray) { "Value for 'anone' operator must be a BsonArray" },
                Filter.ArrayCompType.NONE
            )
            else -> throw IllegalArgumentException("Unsupported operator: $operator")
        }
    }
}