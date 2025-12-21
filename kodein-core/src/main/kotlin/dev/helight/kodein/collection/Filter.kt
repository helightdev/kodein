package dev.helight.kodein.collection

import dev.helight.kodein.collection.Update
import dev.helight.kodein.compareBsonValues
import dev.helight.kodein.getEmbedded
import org.bson.BsonArray
import org.bson.BsonDocument
import org.bson.BsonValue

sealed class Filter {
    class And(val filters: List<Filter>) : Filter()
    class Or(val filters: List<Filter>) : Filter()
    class Not(val filter: Filter) : Filter()

    fun eval(document: BsonDocument): Boolean {
        return when (this) {
            is And -> filters.all { it.eval(document) }
            is Or -> filters.any { it.eval(document) }
            is Not -> !filter.eval(document)
            is Field -> this.evalField(document)
        }
    }

    val upsertUpdates: List<Update.Field> by lazy {
        val accumulator = mutableListOf<Field.Eq>()
        extractUpsertEqFilters(this, accumulator)
        accumulator.map { Update.Field.Set(it.path, it.value) }
    }

    private fun extractUpsertEqFilters(filter: Filter, accumulator: MutableList<Field.Eq>) {
        when (filter) {
            is Field.Eq -> accumulator.add(filter)
            is And -> filter.filters.forEach { extractUpsertEqFilters(it, accumulator) }
            else -> {}
        }
    }

    sealed class Field(
        val path: String,
    ) : Filter() {
        abstract val value: BsonValue

        class Eq(path: String, override val value: BsonValue) : Field(path) {
            override fun evalField(document: BsonDocument): Boolean {
                val fieldValue = document.getEmbedded(path)
                return fieldValue == value
            }
        }

        class Ne(path: String, override val value: BsonValue) : Field(path) {
            override fun evalField(document: BsonDocument): Boolean {
                val fieldValue = document.getEmbedded(path)
                return fieldValue != value
            }
        }

        class In(path: String, override val value: BsonArray) : Field(path) {
            override fun evalField(document: BsonDocument): Boolean {
                val fieldValue = document.getEmbedded(path)
                return value.contains(fieldValue)
            }
        }

        class Nin(path: String, override val value: BsonArray) : Field(path) {
            override fun evalField(document: BsonDocument): Boolean {
                val fieldValue = document.getEmbedded(path)
                return !value.contains(fieldValue)
            }
        }

        class Comp(path: String, override val value: BsonValue, val type: CompType) : Field(path) {
            override fun evalField(document: BsonDocument): Boolean {
                val fieldValue = document.getEmbedded(path) ?: return false
                val cmp = compareBsonValues(fieldValue, value)
                return when (type) {
                    CompType.GT -> cmp > 0
                    CompType.GTE -> cmp >= 0
                    CompType.LT -> cmp < 0
                    CompType.LTE -> cmp <= 0
                }
            }
        }

        abstract fun evalField(document: BsonDocument): Boolean
    }

    enum class CompType { GT, GTE, LT, LTE }
}