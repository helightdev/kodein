package dev.helight.kodein.dsl

import dev.helight.kodein.BsonMarshaller
import dev.helight.kodein.spec.CollectionSpec
import dev.helight.kodein.collection.DocumentCollection
import dev.helight.kodein.spec.DumbFieldSpec
import dev.helight.kodein.collection.Filter
import dev.helight.kodein.spec.ArrayFieldSpec
import dev.helight.kodein.spec.CrudCollection
import dev.helight.kodein.spec.PrimitiveFieldSpec
import dev.helight.kodein.spec.SpecDsl
import dev.helight.kodein.spec.TypedCollectionSpec
import org.bson.BsonArray

interface DumbFieldSpecFilterBuilder : FilterBuilderBase {
    infix fun DumbFieldSpec.eq(value: Any?) {
        filterList.add(Filter.Field.Eq(this.name, BsonMarshaller.marshal(value)))
    }

    infix fun DumbFieldSpec.notEq(value: Any?) {
        filterList.add(Filter.Field.Ne(this.name, BsonMarshaller.marshal(value)))
    }

    infix fun DumbFieldSpec.inList(values: Collection<Any?>) {
        filterList.add(Filter.Field.In(this.name, BsonMarshaller.marshal(values) as BsonArray))
    }

    infix fun DumbFieldSpec.notInList(values: Collection<Any?>) {
        filterList.add(Filter.Field.Nin(this.name, BsonMarshaller.marshal(values) as BsonArray))
    }

    infix fun DumbFieldSpec.gt(value: Any?) {
        filterList.add(Filter.Field.Comp(this.name, BsonMarshaller.marshal(value), Filter.CompType.GT))
    }

    infix fun DumbFieldSpec.gte(value: Any?) {
        filterList.add(Filter.Field.Comp(this.name, BsonMarshaller.marshal(value), Filter.CompType.GTE))
    }

    infix fun DumbFieldSpec.lt(value: Any?) {
        filterList.add(Filter.Field.Comp(this.name, BsonMarshaller.marshal(value), Filter.CompType.LT))
    }

    infix fun DumbFieldSpec.lte(value: Any?) {
        filterList.add(Filter.Field.Comp(this.name, BsonMarshaller.marshal(value), Filter.CompType.LTE))
    }
}

interface TypeAwareFieldSpecFilterBuilder : FilterBuilderBase {
    infix fun <T : Any> PrimitiveFieldSpec<T>.eq(value: T?) {
        filterList.add(Filter.Field.Eq(this.name, BsonMarshaller.marshal(value)))
    }

    infix fun <T : Any> PrimitiveFieldSpec<T>.notEq(value: T?) {
        filterList.add(Filter.Field.Ne(this.name, BsonMarshaller.marshal(value)))
    }

    infix fun <T : Any> PrimitiveFieldSpec<T>.inList(values: Collection<T?>) {
        filterList.add(Filter.Field.In(this.name, BsonMarshaller.marshal(values) as BsonArray))
    }

    infix fun <T : Any> PrimitiveFieldSpec<T>.notInList(values: Collection<T?>) {
        filterList.add(Filter.Field.Nin(this.name, BsonMarshaller.marshal(values) as BsonArray))
    }

    infix fun <T : Any> PrimitiveFieldSpec<T>.gt(value: T?) {
        filterList.add(Filter.Field.Comp(this.name, BsonMarshaller.marshal(value), Filter.CompType.GT))
    }

    infix fun <T : Any> PrimitiveFieldSpec<T>.gte(value: T?) {
        filterList.add(Filter.Field.Comp(this.name, BsonMarshaller.marshal(value), Filter.CompType.GTE))
    }

    infix fun <T : Any> PrimitiveFieldSpec<T>.lt(value: T?) {
        filterList.add(Filter.Field.Comp(this.name, BsonMarshaller.marshal(value), Filter.CompType.LT))
    }

    infix fun <T : Any> PrimitiveFieldSpec<T>.lte(value: T?) {
        filterList.add(Filter.Field.Comp(this.name, BsonMarshaller.marshal(value), Filter.CompType.LTE))
    }
}

interface ArrayFieldSpecFilterBuilder : FilterBuilderBase {
    infix fun <I> ArrayFieldSpec<I, *>.eq(collection: Collection<I>?) {
        filterList.add(Filter.Field.Eq(this.name, BsonMarshaller.marshal(collection)))
    }

    infix fun <I> ArrayFieldSpec<I, *>.notEq(collection: Collection<I>?) {
        filterList.add(Filter.Field.Ne(this.name, BsonMarshaller.marshal(collection)))
    }

    infix fun <I> ArrayFieldSpec<I, *>.contains(value: I?) {
        filterList.add(Filter.Field.ArrCont(this.name, BsonMarshaller.marshal(value)))
    }

    infix fun <I> ArrayFieldSpec<I, *>.notContains(value: I?) {
        filterList.add(Filter.Field.ArrNotCont(this.name, BsonMarshaller.marshal(value)))
    }

    infix fun <I> ArrayFieldSpec<I, *>.intersects(values: Collection<I>) {
        filterList.add(
            Filter.Field.ArrComp(
                this.name,
                BsonMarshaller.marshal(values) as BsonArray,
                Filter.ArrayCompType.ANY
            )
        )
    }

    infix fun <I> ArrayFieldSpec<I, *>.notIntersects(values: Collection<I>) {
        filterList.add(
            Filter.Field.ArrComp(
                this.name,
                BsonMarshaller.marshal(values) as BsonArray,
                Filter.ArrayCompType.NONE
            )
        )
    }

    infix fun <I> ArrayFieldSpec<I, *>.containsAll(values: Collection<I>) {
        filterList.add(
            Filter.Field.ArrComp(
                this.name,
                BsonMarshaller.marshal(values) as BsonArray,
                Filter.ArrayCompType.ALL
            )
        )
    }

    infix fun <I> ArrayFieldSpec<I, *>.equalsSet(values: Collection<I>) {
        filterList.add(
            Filter.Field.ArrComp(
                this.name,
                BsonMarshaller.marshal(values) as BsonArray,
                Filter.ArrayCompType.SET
            )
        )
    }

}

@SpecDsl
inline fun <T : CollectionSpec, R> T.scope(
    collection: DocumentCollection,
    block: T.(collection: DocumentCollection) -> R
): R {
    return this.block(collection)
}


@SpecDsl
inline fun <A : Any, T : TypedCollectionSpec<A>, R> T.crudScope(
    collection: DocumentCollection,
    block: T.(collection: CrudCollection<A, T>) -> R
): R {
    val collection = CrudCollection(collection, this)
    return this.block(collection)
}