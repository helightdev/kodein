package dev.helight.kodein.spec

import kotlin.reflect.KClass

@DslMarker
annotation class SpecDsl

abstract class FieldSpec(
    val name: String,
) {

    operator fun div(other: String): ConcatenatedFieldSpec {
        return ConcatenatedFieldSpec(this.name, other)
    }

    operator fun div(other: FieldSpec): ConcatenatedFieldSpec {
        return ConcatenatedFieldSpec(this.name, other.name)
    }

    operator fun <T : Any> div(other: TypeAwareFieldSpec<T>): TypeAwareConcatenatedFieldSpec<T> {
        return TypeAwareConcatenatedFieldSpec(this.name, other.name, other.type)
    }

}

open class DumbFieldSpec(
    name: String,
) : FieldSpec(name)

open class TypeAwareFieldSpec<T : Any>(
    name: String,
    val type: KClass<T>,
) : FieldSpec(name)

open class PrimitiveFieldSpec<T : Any>(
    name: String,
    type: KClass<T>,
) : TypeAwareFieldSpec<T>(name, type)

class ConcatenatedFieldSpec(
    parent: String,
    name: String,
) : DumbFieldSpec("$parent.$name")

class TypeAwareConcatenatedFieldSpec<T : Any>(
    parent: String,
    name: String,
    type: KClass<T>,
) : PrimitiveFieldSpec<T>("$parent.$name", type)

class CollectionFieldSpec<T : Any>(
    name: String,
    type: KClass<T>,
    var indexType: FieldIndexType = FieldIndexType.NONE,
    var indexName: String? = null,
    var hasTextIndex: Boolean = false
) : PrimitiveFieldSpec<T>(name, type) {
    fun unique(indexName: String? = null): CollectionFieldSpec<T> {
        indexType = FieldIndexType.UNIQUE
        this.indexName = indexName
        return this
    }

    fun indexed(indexName: String? = null): CollectionFieldSpec<T> {
        indexType = FieldIndexType.INDEXED
        this.indexName = indexName
        return this
    }

    companion object {
        fun CollectionFieldSpec<String>.textIndexed(): CollectionFieldSpec<String> {
            this.hasTextIndex = true
            return this
        }
    }
}

class EmbeddedFieldSpec<T : Any, Spec : TypedCollectionSpec<T>>(
    name: String,
    type: KClass<T>,
    val spec: Spec
) : TypeAwareFieldSpec<T>(name, type), FieldNameProducer {

    fun <T : Any> select(selector: Spec.() -> TypeAwareFieldSpec<T>): TypeAwareConcatenatedFieldSpec<T> {
        val fieldSpec = selector(spec)
        return TypeAwareConcatenatedFieldSpec(this.name, fieldSpec.name, fieldSpec.type)
    }

    fun <TA : Any, TS : TypedCollectionSpec<TA>, A : EmbeddedFieldSpec<TA, TS>> cd(selector: Spec.() -> A): EmbeddedFieldSpec<TA, TS> {
        val selected = selector(spec)
        return EmbeddedFieldSpec(
            name = "${this.name}.${selected.name}",
            type = selected.type,
            spec = selected.spec
        )
    }

    override fun getFieldsNames(): Collection<String> = spec.getFieldsNames().map { "${this.name}.${it}" }
}

class ArrayFieldSpec<I : Any, T : Collection<I>>(
    name: String,
    type: KClass<T>,
    val elementType: KClass<I>,
    var indexType: FieldIndexType = FieldIndexType.NONE,
    var indexName: String? = null,
) : TypeAwareFieldSpec<T>(name, type) {

    fun indexed(indexName: String? = null): ArrayFieldSpec<I, T> {
        indexType = FieldIndexType.INDEXED
        this.indexName = indexName
        return this
    }
}

enum class FieldIndexType {
    NONE,
    INDEXED,
    UNIQUE
}