package dev.helight.kodein.collection

import dev.helight.kodein.compareBsonValues
import dev.helight.kodein.getEmbedded
import org.bson.BsonArray
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonNumber
import org.bson.BsonString
import org.bson.BsonValue

sealed class Filter {
    class And(val filters: List<Filter>) : Filter()
    class Or(val filters: List<Filter>) : Filter()
    class Not(val filter: Filter) : Filter()
    class Native(val value: Any) : Filter()
    
    /**
     * Text search filter - searches across all text-indexed fields
     * Similar to MongoDB's $text operator
     */
    class Text(
        val searchTerm: BsonString,
        val indexedFields: Set<String>? = null
    ) : Filter() {
        // Cache the lowercase search term for better performance
        private val searchTermLowercase = searchTerm.value.lowercase()
        
        /**
         * Evaluate text search across text-indexed fields or all string fields
         */
        fun evalText(document: BsonDocument): Boolean {
            // If indexedFields is specified, only search those fields
            if (indexedFields != null && indexedFields.isNotEmpty()) {
                return indexedFields.any { fieldPath ->
                    val fieldValue = document.getEmbedded(fieldPath)
                    if (fieldValue is BsonString) {
                        fieldValue.value.lowercase().contains(searchTermLowercase)
                    } else {
                        false
                    }
                }
            }
            
            // Otherwise, search through all string fields in the document
            return document.values.any { value ->
                if (value is BsonString) {
                    value.value.lowercase().contains(searchTermLowercase)
                } else {
                    false
                }
            }
        }
    }


    fun eval(document: BsonDocument): Boolean {
        return when (this) {
            is And -> filters.all { it.eval(document) }
            is Or -> filters.any { it.eval(document) }
            is Not -> !filter.eval(document)
            is Field -> this.evalField(document)
            is Text -> this.evalText(document)
            is Native -> throw UnsupportedOperationException("Native filter evaluation is not supported")
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

        companion object {
            @JvmStatic
            private fun eq(a: BsonValue, b: BsonValue): Boolean {
                if (a is BsonNumber && b is BsonNumber) {
                    return compareBsonValues(a, b) == 0
                }
                return a == b
            }
        }

        class Eq(path: String, override val value: BsonValue) : Field(path) {
            override fun evalField(document: BsonDocument): Boolean {
                val fieldValue = document.getEmbedded(path) ?: return false
                return eq(fieldValue, value)
            }
        }

        class Ne(path: String, override val value: BsonValue) : Field(path) {
            override fun evalField(document: BsonDocument): Boolean {
                val fieldValue = document.getEmbedded(path) ?: return true
                return !eq(fieldValue, value)
            }
        }

        class In(path: String, override val value: BsonArray) : Field(path) {
            override fun evalField(document: BsonDocument): Boolean {
                val fieldValue = document.getEmbedded(path) ?: return false
                //if (fieldValue is BsonArray && fieldValue.any { value.contains(it) }) return true
                return value.any { eq(it, fieldValue) }
            }
        }

        class Nin(path: String, override val value: BsonArray) : Field(path) {
            override fun evalField(document: BsonDocument): Boolean {
                val fieldValue = document.getEmbedded(path) ?: return true
                //if (fieldValue is BsonArray && fieldValue.any { value.contains(it) }) return false
                return !value.any { eq(it, fieldValue) }
            }
        }

        class Comp(path: String, override val value: BsonValue, val type: CompType) : Field(path) {
            override fun evalField(document: BsonDocument): Boolean {
                val fieldValue = document.getEmbedded(path) ?: return false
                //if (fieldValue is BsonArray) return fieldValue.any { comp(it) }
                return comp(fieldValue)
            }

            private fun comp(fieldValue: BsonValue): Boolean {
                val comparison = compareBsonValues(fieldValue, value)
                return when (type) {
                    CompType.GT -> comparison > 0
                    CompType.GTE -> comparison >= 0
                    CompType.LT -> comparison < 0
                    CompType.LTE -> comparison <= 0
                }
            }
        }

        class ArrCont(path: String, override val value: BsonValue) : Field(path) {
            override fun evalField(document: BsonDocument): Boolean {
                val fieldValue = document.getEmbedded(path) ?: return false
                if (fieldValue !is BsonArray) return false
                return fieldValue.any { eq(it, value) }
            }
        }

        class ArrNotCont(path: String, override val value: BsonValue) : Field(path) {
            override fun evalField(document: BsonDocument): Boolean {
                val fieldValue = document.getEmbedded(path) ?: return true
                if (fieldValue !is BsonArray) return true
                return fieldValue.none { eq(it, value) }
            }
        }

        class ArrSize(path: String, override val value: BsonInt32) : Field(path) {
            override fun evalField(document: BsonDocument): Boolean {
                val fieldValue = document.getEmbedded(path) ?: return false
                if (fieldValue !is BsonArray) return false
                return fieldValue.size == value.intValue()
            }
        }

        class ArrComp(path: String, override val value: BsonArray, val type: ArrayCompType) : Field(path) {
            override fun evalField(document: BsonDocument): Boolean {
                val fieldValue = document.getEmbedded(path) ?: return true
                if (fieldValue !is BsonArray) {
                    return type == ArrayCompType.NONE
                }
                val intersection = fieldValue.filter { fv -> value.any { eq(it, fv) } }
                return when (type) {
                    ArrayCompType.ANY -> intersection.isNotEmpty()
                    ArrayCompType.NONE -> intersection.isEmpty()
                    ArrayCompType.ALL -> intersection.size == value.size
                    ArrayCompType.SET -> intersection.size == fieldValue.size && intersection.size == value.size
                }
            }

        }

        abstract fun evalField(document: BsonDocument): Boolean
    }

    enum class CompType { GT, GTE, LT, LTE }

    enum class ArrayCompType { ANY, NONE, ALL, SET  }
}