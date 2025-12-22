package dev.helight.kodein.dsl

import dev.helight.kodein.BsonMarshaller
import dev.helight.kodein.Kodein
import dev.helight.kodein.spec.DumbFieldSpec
import dev.helight.kodein.spec.EmbeddedFieldSpec
import dev.helight.kodein.spec.FieldNameProducer
import dev.helight.kodein.spec.FieldSpec
import dev.helight.kodein.collection.Filter
import dev.helight.kodein.collection.FindOptions
import dev.helight.kodein.spec.PrimitiveFieldSpec
import dev.helight.kodein.spec.TypedCollectionSpec
import dev.helight.kodein.collection.Update
import org.bson.BsonArray
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonNull
import org.bson.BsonValue

@DslMarker
annotation class QueryDsl

fun buildFilter(block: FilterBuilder.() -> Unit): Filter {
    val builder = FilterBuilderImpl()
    builder.block()
    return builder.build()
}

fun idFilter(id: Any?): Filter {
    return Filter.Field.Eq("_id", BsonMarshaller.marshal(id))
}

fun idListFilter(ids: Collection<Any?>): Filter {
    return Filter.Field.In("_id", BsonMarshaller.marshal(ids) as BsonArray)
}

fun findOptions(block: FindOptionsBuilder.() -> Unit): FindOptions {
    val builder = FindOptionsBuilderImpl()
    builder.block()
    return builder.build()
}

fun buildUpdate(block: UpdateBuilder.() -> Unit): Update {
    val builder = UpdateBuilderImpl()
    builder.block()
    return builder.build()
}

interface FilterBuilderBase {

    val filterList: MutableList<Filter>
}

@QueryDsl
interface FilterBuilder : FilterBuilderBase, DumbFieldSpecFilterBuilder, TypeAwareFieldSpecFilterBuilder, ArrayFieldSpecFilterBuilder {

    fun addNative(value: Any) {
        filterList.add(Filter.Native(value))
    }

    fun and(block: FilterBuilder.() -> Unit) {
        val builder = FilterBuilderImpl()
        builder.block()
        filterList.add(andUnwrap(builder.filterList))
    }

    fun or(block: FilterBuilder.() -> Unit) {
        val builder = FilterBuilderImpl()
        builder.block()
        filterList.add(orUnwarp(builder.filterList))
    }

    fun not(block: FilterBuilder.() -> Unit) {
        val builder = FilterBuilderImpl()
        builder.block()
        if (builder.filterList.isEmpty()) return
        filterList.add(Filter.Not(andUnwrap(builder.filterList)))
    }

    //<editor-fold desc="Generic Field DSL">
    fun byId(id: Any?) {
        filterList.add(Filter.Field.Eq("_id", BsonMarshaller.marshal(id)))
    }

    infix fun String.eq(value: Any?) {
        filterList.add(Filter.Field.Eq(this, BsonMarshaller.marshal(value)))
    }

    infix fun String.notEq(value: Any?) {
        filterList.add(Filter.Field.Ne(this, BsonMarshaller.marshal(value)))
    }

    infix fun String.inList(values: Collection<Any?>) {
        filterList.add(Filter.Field.In(this, BsonMarshaller.marshal(values) as BsonArray))
    }

    infix fun String.notInList(values: Collection<Any?>) {
        filterList.add(Filter.Field.Nin(this, BsonMarshaller.marshal(values) as BsonArray))
    }

    infix fun String.gt(value: Any?) {
        filterList.add(Filter.Field.Comp(this, BsonMarshaller.marshal(value), Filter.CompType.GT))
    }

    infix fun String.gte(value: Any?) {
        filterList.add(Filter.Field.Comp(this, BsonMarshaller.marshal(value), Filter.CompType.GTE))
    }

    infix fun String.lt(value: Any?) {
        filterList.add(Filter.Field.Comp(this, BsonMarshaller.marshal(value), Filter.CompType.LT))
    }

    infix fun String.lte(value: Any?) {
        filterList.add(Filter.Field.Comp(this, BsonMarshaller.marshal(value), Filter.CompType.LTE))
    }

    infix fun String.inRange(range: ClosedRange<*>) {
        filterList.add(
            Filter.And(
                listOf(
                    Filter.Field.Comp(this, BsonMarshaller.marshal(range.start), Filter.CompType.GTE),
                    Filter.Field.Comp(this, BsonMarshaller.marshal(range.endInclusive), Filter.CompType.LTE)
                )
            )
        )
    }

    infix fun String.inRange(range: OpenEndRange<*>) {
        filterList.add(
            Filter.And(
                listOf(
                    Filter.Field.Comp(this, BsonMarshaller.marshal(range.start), Filter.CompType.GTE),
                    Filter.Field.Comp(this, BsonMarshaller.marshal(range.endExclusive), Filter.CompType.LT)
                )
            )
        )
    }

    infix fun String.inRange(range: IntRange) {
        filterList.add(
            Filter.And(
                listOf(
                    Filter.Field.Comp(this, BsonInt32(range.first), Filter.CompType.GTE),
                    Filter.Field.Comp(this, BsonInt32(range.last), Filter.CompType.LTE)
                )
            )
        )
    }

    infix fun String.contains(value: Any?) {
        filterList.add(Filter.Field.ArrCont(this, BsonMarshaller.marshal(value)))
    }

    infix fun String.notContains(value: Any?) {
        filterList.add(Filter.Field.ArrNotCont(this, BsonMarshaller.marshal(value)))
    }

    infix fun String.intersects(values: Collection<Any?>) {
        filterList.add(
            Filter.Field.ArrComp(
                this,
                BsonMarshaller.marshal(values) as BsonArray,
                Filter.ArrayCompType.ANY
            )
        )
    }

    infix fun String.notIntersects(values: Collection<Any?>) {
        filterList.add(
            Filter.Field.ArrComp(
                this,
                BsonMarshaller.marshal(values) as BsonArray,
                Filter.ArrayCompType.NONE
            )
        )
    }

    infix fun String.containsAll(values: Collection<Any?>) {
        filterList.add(
            Filter.Field.ArrComp(
                this,
                BsonMarshaller.marshal(values) as BsonArray,
                Filter.ArrayCompType.ALL
            )
        )
    }

    infix fun String.equalsSet(values: Collection<Any?>) {
        filterList.add(
            Filter.Field.ArrComp(
                this,
                BsonMarshaller.marshal(values) as BsonArray,
                Filter.ArrayCompType.SET
            )
        )
    }


    //</editor-fold>

    companion object {
        fun andUnwrap(filter: List<Filter>): Filter {
            return if (filter.size == 1) filter[0] else Filter.And(filter)
        }

        fun orUnwarp(filter: List<Filter>): Filter {
            return if (filter.size == 1) filter[0] else Filter.Or(filter)
        }
    }
}

@QueryDsl
class FilterBuilderImpl : FilterBuilder {
    override val filterList: MutableList<Filter> = mutableListOf()

    fun build(): Filter = FilterBuilder.andUnwrap(filterList)
}

@QueryDsl
class FindBuilder : FindOptionsBuilder, FilterBuilder {
    override var skip: Int? = null
    override var limit: Int? = null
    override val sorts: MutableList<FindOptions.Sort> = mutableListOf()
    override val fields: MutableSet<String> = mutableSetOf()
    override val filterList: MutableList<Filter> = mutableListOf()

    fun options(block: FindOptionsBuilder.() -> Unit) {
        block()
    }

    fun where(block: FilterBuilder.() -> Unit) {
        block()
    }

    internal fun build(): Pair<Filter, FindOptions> = Pair(
        FilterBuilder.andUnwrap(filterList),
        FindOptions(
            skip = skip ?: 0,
            limit = limit ?: Int.MAX_VALUE,
            sort = sorts,
            fields = fields
        )
    )
}


@QueryDsl
interface FindOptionsBuilder {
    var skip: Int?
    var limit: Int?
    val sorts: MutableList<FindOptions.Sort>
    val fields: MutableSet<String>

    operator fun String.unaryPlus() {
        sorts.add(FindOptions.Sort(this, ascending = true))
    }

    fun sortAsc(path: String): FindOptionsBuilder {
        sorts.add(FindOptions.Sort(path, ascending = true))
        return this
    }

    fun sortAsc(spec: FieldSpec) = sortAsc(spec.name)

    operator fun String.unaryMinus() {
        sorts.add(FindOptions.Sort(this, ascending = false))
    }

    fun sortDesc(path: String): FindOptionsBuilder {
        sorts.add(FindOptions.Sort(path, ascending = false))
        return this
    }

    fun sortDesc(spec: FieldSpec) = sortDesc(spec.name)


    fun fields(vararg paths: String): FindOptionsBuilder {
        fields.addAll(paths)
        return this
    }

    fun fields(producer: FieldNameProducer): FindOptionsBuilder {
        fields.addAll(producer.getFieldsNames())
        return this
    }

    fun skip(value: Int): FindOptionsBuilder {
        skip = value
        return this
    }

    fun limit(value: Int): FindOptionsBuilder {
        limit = value
        return this
    }
}

@QueryDsl
class FindOptionsBuilderImpl : FindOptionsBuilder {

    override var skip: Int? = null
    override var limit: Int? = null
    override var sorts: MutableList<FindOptions.Sort> = mutableListOf()
    override var fields: MutableSet<String> = mutableSetOf()

    fun build(): FindOptions = FindOptions(
        skip = skip ?: 0,
        limit = limit ?: Int.MAX_VALUE,
        sort = sorts,
        fields = fields
    )
}

@QueryDsl
class UpdateBuilderImpl : UpdateBuilder {
    override val fieldUpdates = mutableListOf<Update.Field>()
    override var upsert: Boolean = false

    fun build(): Update = Update(fieldUpdates, upsert)
}

interface UpdateBuilderBase {
    val fieldUpdates: MutableList<Update.Field>
    val kodein: Kodein?
        get() = null
}

interface FieldSpecUpdateBuilder : UpdateBuilderBase {

    fun unset(vararg specs: FieldSpec) {
        for (spec in specs) {
            fieldUpdates.add(Update.Field.Unset(spec.name))
        }
    }

    infix fun DumbFieldSpec.set(value: Any?) {
        fieldUpdates.add(Update.Field.Set(this.name, BsonMarshaller.marshal(value)))
    }

    infix fun DumbFieldSpec.setDefault(value: Any?) {
        fieldUpdates.add(Update.Field.SetOnInsert(this.name, BsonMarshaller.marshal(value)))
    }

    infix fun FieldSpec.inc(value: Number) {
        fieldUpdates.add(Update.Field.Inc(this.name, value))
    }

    infix fun <T : Any> PrimitiveFieldSpec<T>.set(value: T?) {
        fieldUpdates.add(Update.Field.Set(this.name, BsonMarshaller.marshal(value)))
    }

    infix fun <T : Any> PrimitiveFieldSpec<T>.setDefault(value: T?) {
        fieldUpdates.add(Update.Field.SetOnInsert(this.name, BsonMarshaller.marshal(value)))
    }

    private fun <T : Any, S : TypedCollectionSpec<T>> encodeEmbedded(value: T?, spec: S): BsonValue = when {
        kodein == null -> throw IllegalStateException("Cannot use EmbeddedFieldSpec.set without kodein context")
        value == null -> BsonNull.VALUE
        else -> kodein!!.encode(value, spec.clazz.java)
    }

    infix fun <T : Any, S : TypedCollectionSpec<T>> EmbeddedFieldSpec<T, S>.set(value: T?) {
        val value = encodeEmbedded(value, this.spec)
        fieldUpdates.add(Update.Field.Set(this.name, value))
    }

    infix fun <T : Any, S : TypedCollectionSpec<T>> EmbeddedFieldSpec<T, S>.setDefault(value: T?) {
        val value = encodeEmbedded(value, this.spec)
        fieldUpdates.add(Update.Field.SetOnInsert(this.name, value))
    }
}

@QueryDsl
interface UpdateBuilder : UpdateBuilderBase, FieldSpecUpdateBuilder {
    var upsert: Boolean

    infix fun String.set(value: Any?) {
        fieldUpdates.add(Update.Field.Set(this, BsonMarshaller.marshal(value)))
    }

    infix fun String.setDefault(value: Any?) {
        fieldUpdates.add(Update.Field.SetOnInsert(this, BsonMarshaller.marshal(value)))
    }

    fun unset(vararg paths: String) {
        for (path in paths) {
            fieldUpdates.add(Update.Field.Unset(path))
        }
    }

    fun setAll(other: BsonDocument) {
        other.forEach { (key, value) ->
            fieldUpdates.add(Update.Field.Set(key, value))
        }
    }

    infix fun String.inc(value: Number) {
        fieldUpdates.add(Update.Field.Inc(this, value))
    }

    operator fun String.unaryMinus() {
        fieldUpdates.add(Update.Field.Unset(this))
    }
}

@QueryDsl
class SelectiveUpdateBuilder(
    override val kodein: Kodein? = null
) : UpdateBuilder {
    var filter: Filter? = null
    override val fieldUpdates = mutableListOf<Update.Field>()
    override var upsert: Boolean = false

    fun upsert(): SelectiveUpdateBuilder {
        upsert = true
        return this
    }

    fun where(block: FilterBuilder.() -> Unit) {
        val builder = FilterBuilderImpl()
        builder.block()
        filter = FilterBuilder.andUnwrap(builder.filterList)
    }

    fun whereId(id: Any?): SelectiveUpdateBuilder {
        filter = Filter.Field.Eq("_id", BsonMarshaller.marshal(id))
        return this
    }

    fun update(block: UpdateBuilder.() -> Unit) {
        block()
    }

    internal fun build(): Pair<Filter, Update> {
        return Pair(
            filter ?: Filter.And(listOf()),
            Update(fieldUpdates, upsert)
        )
    }
}