package dev.helight.kodein

import dev.helight.kodein.BsonMarshaller.toBson
import dev.helight.kodein.collection.Filter
import dev.helight.kodein.spec.CollectionSpec
import dev.helight.kodein.spec.FieldNameProducer
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.jsonPrimitive
import org.bson.BsonArray
import org.bson.BsonObjectId
import org.bson.BsonValue
import org.bson.types.ObjectId

data class DynamicFilters(
    val permittedFields: Set<String>? = null,
    val permittedOperations: Set<String>? = null,
    val replacements: Map<String, String> = emptyMap(),
    val transformers: Map<String, (JsonElement) -> BsonValue> = emptyMap(),
) {
    companion object {
        val default = DynamicFilters()

        fun forSpecs(vararg spec: FieldNameProducer, replacements: Map<String, String> = emptyMap()): DynamicFilters {
            val permissible = spec.flatMap { it.getFieldsNames() }.toSet()
            return DynamicFilters(permissible, replacements = replacements)
        }

        fun forCollection(
            spec: CollectionSpec, replacements: Map<String, String> = mapOf("id" to "_id")
        ): DynamicFilters {
            val permissible = spec.getFieldsNames().toSet()
            return DynamicFilters(permissible + setOf("_id", "id"), replacements = replacements,
                transformers = mapOf("_id" to ::transformBsonId)
            )
        }

        fun transformBsonId(jsonElement: JsonElement): BsonValue {
            return BsonObjectId(ObjectId(jsonElement.jsonPrimitive.content))
        }
    }

    fun parseAll(strings: List<String>): Filter {
        val filters = strings.map { parseSingle(it) }
        return if (filters.size == 1) filters[0] else Filter.And(filters)
    }

    fun parseSingle(str: String): Filter {
        val it = str.split(":")
        if (it.size !in 2..3) throw IllegalArgumentException("Invalid filter format: $str")
        var key = it[0].trim()
        if (key in replacements) {
            key = replacements[key]!!
        }
        if (permittedFields != null && key !in permittedFields) {
            throw IllegalArgumentException("Field '$key' is not permitted in filters")
        }
        val operator = if (it.size == 3) it[1].trim() else "eq"
        val valueElement = Json.parseToJsonElement(it.last())
        val value = transformers[key]?.invoke(valueElement) ?: valueElement.toBson()

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