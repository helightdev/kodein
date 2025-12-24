package dev.helight.kodein.spec

import dev.helight.kodein.BsonMarshaller
import dev.helight.kodein.collection.DocumentCollection
import dev.helight.kodein.collection.KPage
import dev.helight.kodein.collection.KPageCursor
import dev.helight.kodein.dsl.FilterBuilder
import dev.helight.kodein.dsl.FindBuilder
import dev.helight.kodein.dsl.SelectiveUpdateBuilder
import dev.helight.kodein.dsl.UpdateBuilder
import dev.helight.kodein.dsl.buildFilter
import dev.helight.kodein.dsl.buildUpdate
import dev.helight.kodein.dsl.idFilter
import dev.helight.kodein.dsl.idListFilter
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import org.bson.BsonDocument
import org.bson.BsonObjectId

class CrudCollection<T : Any, Spec : TypedCollectionSpec<T>>(
    val untyped: DocumentCollection,
    internal val spec: Spec
) {
    private val clazz = spec.clazz.java

    private fun decode(document: BsonDocument): T {
        val decoded = untyped.kodein.decode(document, clazz)
        if (decoded is BaseDocument) {
            decoded.bsonId = document["_id"]
            decoded.document = untyped.kodein.introspect(document)
        }
        return decoded
    }

    private fun encode(value: T, generateId: Boolean = true): BsonDocument {
        val document = untyped.kodein.encode(value, clazz)
        if (generateId) when (value) {
            is BaseDocument -> document["_id"] = value.bsonId ?: BsonObjectId()
            else -> document["_id"] = BsonObjectId()
        }
        return document
    }

    suspend fun insert(value: T): T {
        val document = encode(value)
        val success = untyped.insert(document)
        if (!success) throw CrudException("Failed to insert document")
        return decode(document)
    }

    suspend fun insertMany(values: List<T>): List<T> {
        val documents = values.map { encode(it) }
        val insertedCount = untyped.insert(documents)
        if (insertedCount != documents.size) throw CrudException("Failed to insert all documents")
        return documents.map { decode(it) }
    }

    suspend fun save(value: T): T {
        val document = encode(value, true)
        val success = untyped.replace(
            filter = buildFilter { byId(document["_id"]) },
            document = document,
            upsert = true
        )
        if (!success) throw CrudException("Failed to save document")
        return decode(document)
    }

    suspend fun save(original: T, modify: T.() -> T): T {
        require(original is BaseDocument) { "Can only modify->save documents extending BaseDocument" }
        val modified = modify(original)
        (modified as BaseDocument).bsonId = original.bsonId
        return save(modified)
    }

    suspend fun count(block: FilterBuilder.() -> Unit = {}): Long {
        return untyped.count(block)
    }

    suspend fun exists(block: FilterBuilder.() -> Unit = {}): Boolean {
        return untyped.exists(block)
    }

    suspend fun delete(block: FilterBuilder.() -> Unit): Int {
        return untyped.delete(block)
    }

    suspend fun deleteById(id: BsonObjectId): Boolean {
        return untyped.deleteOne(buildFilter { byId(id) })
    }

    suspend fun deleteByIds(ids: List<BsonObjectId>): Int {
        return untyped.delete(idListFilter(ids))
    }

    suspend fun delete(value: T) {
        require(value is BaseDocument) { "Can only delete documents extending BaseDocument" }
        requireNotNull(value.bsonId)
        val success = untyped.deleteOne(buildFilter { byId(value.bsonId) })
        if (!success) throw CrudException("Failed to delete document")
    }

    suspend fun deleteMany(values: List<T>) {
        require(values.all { it is BaseDocument && it.bsonId != null }) {
            "Can only delete documents extending BaseDocument with non-null IDs"
        }
        val deletedCount = untyped.delete(filter = idListFilter(values.map { (it as BaseDocument).bsonId!! }))
        if (deletedCount != values.size) throw CrudException("Failed to delete all documents")
    }

    suspend fun update(block: SelectiveUpdateBuilder.() -> Unit): Int {
        return untyped.update(block)
    }

    suspend fun updateOne(block: SelectiveUpdateBuilder.() -> Unit): Boolean {
        return untyped.updateOne(block)
    }

    suspend fun update(value: T, block: UpdateBuilder.() -> Unit) {
        require(value is BaseDocument) { "Can only update documents extending BaseDocument" }
        requireNotNull(value.bsonId)
        val update = buildUpdate(block)
        val success = untyped.updateOne(
            filter = idFilter(value.bsonId),
            update = update
        )
        if (!success) throw CrudException("Failed to update document")
    }

    suspend fun updateReturning(value: T, block: UpdateBuilder.() -> Unit): T? {
        require(value is BaseDocument) { "Can only update documents extending BaseDocument" }
        requireNotNull(value.bsonId)
        val update = buildUpdate(block)
        val updatedDocument = untyped.updateOneReturning(
            filter = idFilter(value.bsonId),
            update = update
        ) ?: return null
        return updatedDocument.asClass(clazz)
    }

    suspend fun updateReturning(block: SelectiveUpdateBuilder.() -> Unit): T? {
        val updatedDocument = untyped.updateOneReturning(block) ?: return null
        return updatedDocument.asClass(clazz)
    }

    suspend fun updateMany(values: List<T>, block: UpdateBuilder.() -> Unit): Int {
        require(values.all { it is BaseDocument && it.bsonId != null }) {
            "Can only update documents extending BaseDocument with non-null IDs"
        }
        return updateByIds(values.map { (it as BaseDocument).bsonId!! }, block = block)
    }

    suspend fun updateByIds(ids: List<Any>, block: UpdateBuilder.() -> Unit): Int = untyped.update(
        filter = idListFilter(ids),
        update = buildUpdate(block)
    )

    suspend fun updateById(id: Any, block: UpdateBuilder.() -> Unit) {
        val bsonId = BsonMarshaller.marshal(id)
        val success = untyped.updateOne {
            whereId(bsonId)
            block()
        }
        if (!success) throw CrudException("Failed to update document")
    }

    suspend fun find(): Flow<T> {
        return untyped.find().map { it.asClass(clazz) }
    }

    suspend fun find(block: FindBuilder.() -> Unit): Flow<T> {
        return untyped.find(block).map { it.asClass(clazz) }
    }

    suspend fun findOne(block: FindBuilder.() -> Unit): T? {
        return untyped.findOne(block)?.asClass(clazz)
    }

    suspend fun findById(any: Any): T? {
        val id = BsonMarshaller.marshal(any)
        val document = untyped.findOne(buildFilter { byId(id) }) ?: return null
        return document.asClass(clazz)
    }

    suspend fun findByIds(ids: List<Any>): List<T> {
        return untyped.find(idListFilter(ids)).map { it.asClass(clazz) }.toList()
    }

    suspend fun findPaginated(cursor: KPageCursor): KPage<T> {
        return untyped.findPaginated(cursor).mapItems { it.asClass(clazz) }
    }

    suspend fun findPaginated(cursor: KPageCursor, block: FindBuilder.() -> Unit): KPage<T> {
        return untyped.findPaginated(cursor, block).mapItems { it.asClass(clazz) }
    }
}

class CrudException(message: String) : Exception(message)