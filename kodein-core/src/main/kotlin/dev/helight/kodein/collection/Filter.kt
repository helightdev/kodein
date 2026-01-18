package dev.helight.kodein.collection

import dev.helight.kodein.compareBsonValues
import dev.helight.kodein.getEmbedded
import org.bson.BsonArray
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonNumber
import org.bson.BsonRegularExpression
import org.bson.BsonValue
import java.util.regex.Pattern

sealed class Filter {
    protected var isOptimized: Boolean = false

    class And(val filters: List<Filter>) : Filter()
    class Or(val filters: List<Filter>) : Filter()
    class Not(val filter: Filter) : Filter()
    class Native(val value: Any) : Filter()


    fun eval(document: BsonDocument): Boolean {
        return when (this) {
            is And -> filters.all { it.eval(document) }
            is Or -> filters.any { it.eval(document) }
            is Not -> !filter.eval(document)
            is Field -> this.evalField(document)
            is Native -> throw UnsupportedOperationException("Native filter evaluation is not supported")
        }
    }

    fun optimize(): Filter {
        if (isOptimized) return this
        return when (this) {
            is And -> {
                val flattened = filters.flatMap {
                    val filter = it.optimize()
                    if (filter is And) filter.filters else listOf(filter)
                }
                flattened.singleOrNull() ?: And(flattened)
            }
            is Or -> {
                val flattened = filters.flatMap {
                    val filter = it.optimize()
                    if (filter is Or) filter.filters else listOf(filter)
                }
                flattened.singleOrNull() ?: Or(flattened)
            }
            is Not -> {
                val inner = filter.optimize()
                if (inner is Not) inner.filter else Not(inner)
            }
            is Field, is Native -> this
        }.apply { isOptimized = true }
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

        class Regex(path: String, override val value: BsonRegularExpression) : Field(path) {
            override fun evalField(document: BsonDocument): Boolean {
                val fieldValue = document.getEmbedded(path) ?: return false
                if (fieldValue is BsonArray) return false
                val target = when (fieldValue) {
                    is org.bson.BsonString -> fieldValue.value
                    else -> return false
                }

                val pattern = value.pattern
                val flags = value.options
                var intFlags = 0
                if (flags.contains("i")) intFlags = intFlags or Pattern.CASE_INSENSITIVE
                if (flags.contains("m")) intFlags = intFlags or Pattern.MULTILINE
                if (flags.contains("s")) intFlags = intFlags or Pattern.DOTALL
                if (flags.contains("x")) intFlags = intFlags or Pattern.COMMENTS
                val compiled = try {
                    Pattern.compile(pattern, intFlags)
                } catch (_: Throwable) {
                    return false
                }

                return compiled.matcher(target).find()
            }

            companion object {
                fun Pattern.toBsonRegex(): BsonRegularExpression {
                    val opts = StringBuilder()
                    val flags = this.flags()
                    if ((flags and Pattern.CASE_INSENSITIVE) != 0) opts.append('i')
                    if ((flags and Pattern.MULTILINE) != 0) opts.append('m')
                    if ((flags and Pattern.DOTALL) != 0) opts.append('s')
                    if ((flags and Pattern.COMMENTS) != 0) opts.append('x')
                    return BsonRegularExpression(this.pattern(), opts.toString())
                }
            }
        }

        abstract fun evalField(document: BsonDocument): Boolean
    }

    enum class CompType { GT, GTE, LT, LTE }

    enum class ArrayCompType { ANY, NONE, ALL, SET  }
}