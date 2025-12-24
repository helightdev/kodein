package dev.helight.kodein.spec

import kotlinx.serialization.SerialName
import kotlin.reflect.KClass
import kotlin.reflect.KProperty
import kotlin.reflect.jvm.jvmErasure

@SpecDsl
abstract class CollectionSpec : FieldNameProducer {
    protected val fieldList = mutableListOf<FieldSpec>()

    fun getFields(): List<FieldSpec> = fieldList
    override fun getFieldsNames(): Collection<String> = fieldList.map { it.name } + "_id"

    fun <T : Any> field(
        name: String,
        type: KClass<T>,
    ): CollectionFieldSpec<T> {
        val element = CollectionFieldSpec(name, type)
        fieldList.add(element)
        return element
    }

    fun <A : Any, ASpec : TypedCollectionSpec<A>> embeddedField(
        name: String,
        spec: ASpec
    ): EmbeddedFieldSpec<A, ASpec> {
        val element = EmbeddedFieldSpec(name, spec.clazz, spec)
        fieldList.add(element)
        return element
    }

    fun <I, T : Collection<I>> arrayField(
        name: String,
        type: KClass<T>,
    ): ArrayFieldSpec<I, T> {
        val element = ArrayFieldSpec(name, type)
        fieldList.add(element)
        return element
    }

    inline fun <reified A : Any> field(name: String) = field(name, A::class)

    private fun getSerialName(property: KProperty<*>): String {
        val annotation = property.annotations.filterIsInstance<SerialName>().firstOrNull()
        return annotation?.value ?: property.name
    }

    @Suppress("UNCHECKED_CAST")
    private fun <T : Any> getFieldType(property: KProperty<T>): KClass<T> = property.returnType.jvmErasure as KClass<T>

    @Suppress("UNCHECKED_CAST")
    @JvmName("getFieldTypeNullable")
    private fun <T : Any> getFieldType(property: KProperty<T?>): KClass<T> = property.returnType.jvmErasure as KClass<T>

    fun <A : Any> field(property: KProperty<A>) = field(getSerialName(property), getFieldType(property))

    @JvmName("fieldNullable")
    fun <A : Any> field(property: KProperty<A?>) = field(getSerialName(property), getFieldType(property))

    fun <A : Any, ASpec : TypedCollectionSpec<A>> embeddedField(
        property: KProperty<A>,
        spec: ASpec
    ) = embeddedField(getSerialName(property), spec)

    @JvmName("embeddedFieldNullable")
    fun <A : Any, ASpec : TypedCollectionSpec<A>> embeddedField(
        property: KProperty<A?>,
        spec: ASpec
    ) = embeddedField(getSerialName(property), spec)

    fun <I, T : Collection<I>> arrayField(
        property: KProperty<T>
    ) = arrayField(getSerialName(property), getFieldType(property))

    @JvmName("arrayFieldNullable")
    fun <I, T : Collection<I>> arrayField(
        property: KProperty<T?>
    ) = arrayField(getSerialName(property), getFieldType(property))

    internal fun collectIndices(textIndices: MutableSet<String>, path: String? = null): Map<String, Pair<String, FieldIndexType>> {
        val indices = mutableMapOf<String, Pair<String, FieldIndexType>>()
        for (field in getFields()) {
            if (field is CollectionFieldSpec<*>) {
                val fieldPath = path?.let { "$it.${field.name}" } ?: field.name
                if (field.hasTextIndex) textIndices.add(fieldPath)
                if (field.indexType == FieldIndexType.NONE) continue
                indices[field.name] = Pair(
                    field.indexName ?: fieldPath.replace(".", "_"),
                    field.indexType
                )
            } else if (field is ArrayFieldSpec<*,*>) {
                val fieldPath = path?.let { "$it.${field.name}" } ?: field.name
                if (field.indexType == FieldIndexType.NONE) continue
                indices[field.name] = Pair(
                    field.indexName ?: fieldPath.replace(".", "_"),
                    field.indexType
                )
            }
            else if (field is EmbeddedFieldSpec<*, *>) {
                val embeddedPath = if (path == null) field.name else "$path.${field.name}"
                val embeddedIndices = field.spec.collectIndices(textIndices, embeddedPath)
                indices.putAll(embeddedIndices)
            }
        }
        return indices
    }

    fun getIndices(): IndexList {
        val textIndices = mutableSetOf<String>()
        val collected = collectIndices(textIndices)
        val indices = collected.map { (fieldPath, pair) ->
            IndexDefinition(
                path = fieldPath,
                indexName = pair.first,
                indexType = pair.second
            )
        }
        return IndexList(
            indices = indices,
            textIndices = textIndices
        )
    }
}

@SpecDsl
abstract class TypedCollectionSpec<T : Any>(
    val clazz: KClass<T>
) : CollectionSpec()

@SpecDsl
abstract class TypedEntitySpec<T : Any>(
    clazz: KClass<T>
) : TypedCollectionSpec<T>(clazz) {
    override fun getFieldsNames(): Collection<String> = fieldList.map { it.name }
}

data class IndexList(
    val indices: List<IndexDefinition>,
    val textIndices: Set<String>
)

data class IndexDefinition(
    val path: String,
    val indexName: String,
    val indexType: FieldIndexType
)