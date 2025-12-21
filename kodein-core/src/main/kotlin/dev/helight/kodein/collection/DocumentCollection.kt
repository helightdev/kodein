package dev.helight.kodein.collection

import dev.helight.kodein.KDocument
import dev.helight.kodein.Kodein
import dev.helight.kodein.dsl.FilterBuilder
import dev.helight.kodein.dsl.FindBuilder
import dev.helight.kodein.dsl.SelectiveUpdateBuilder
import dev.helight.kodein.dsl.buildFilter
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.toList
import org.bson.BsonDocument

interface DocumentCollection {
    val supportsProjections: Boolean
        get() = false

    val kodein: Kodein

    suspend fun insert(document: BsonDocument): Boolean

    suspend fun insert(documents: List<BsonDocument>): Int {
        var i = 0
        for (doc in documents) {
            if (insert(doc)) i++
        }
        return i
    }
    suspend fun insert(vararg documents: BsonDocument): Int {
        return insert(documents.toList())
    }

    suspend fun update(filter: Filter, update: Update): Int
    suspend fun updateOne(filter: Filter, update: Update): Boolean
    suspend fun replace(filter: Filter, document: BsonDocument, upsert: Boolean = false): Boolean
    suspend fun delete(filter: Filter): Int
    suspend fun deleteOne(filter: Filter): Boolean

    suspend fun count(filter: Filter? = null): Long
    suspend fun find(filter: Filter? = null, options: FindOptions = FindOptions()): Flow<KDocument>
    suspend fun findOne(filter: Filter, options: FindOptions = FindOptions()): KDocument?

    suspend fun findPaginated(
        cursor: KPageCursor,
        filter: Filter? = null,
        options: FindOptions = FindOptions()
    ): KPage<KDocument> = coroutineScope {
        val items = async { find(filter, options.copy(skip = cursor.skip, limit = cursor.limit)) }
        val count = async { count(filter) }
        KPage(
            itemCount = count.await(),
            page = cursor.page,
            pageSize = cursor.pageSize,
            items = items.await().toList()
        )
    }

    suspend fun exists(filter: Filter): Boolean {
        return findOne(filter, FindOptions().copy(limit = 1)) != null
    }


    // DSL
    suspend fun count(block: FilterBuilder.() -> Unit): Long = count(buildFilter(block))

    suspend fun find(
        block: FindBuilder.() -> Unit
    ): Flow<KDocument> {
        val (filter,options) = FindBuilder().apply { block() }.build()
        return find(filter, options)
    }

    suspend fun findOne(
        block: FindBuilder.() -> Unit
    ): KDocument? {
        val (filter,options) = FindBuilder().apply { block() }.build()
        return findOne(filter, options)
    }

    suspend fun findPaginated(
        cursor: KPageCursor,
        block: FindBuilder.() -> Unit
    ): KPage<KDocument> {
        val (filter,options) = FindBuilder().apply { block() }.build()
        return findPaginated(cursor, filter, options)
    }

    suspend fun exists(block: FilterBuilder.() -> Unit): Boolean = exists(buildFilter(block))

    suspend fun deleteOne(block: FilterBuilder.() -> Unit): Boolean {
        return deleteOne(buildFilter(block))
    }

    suspend fun delete(block: FilterBuilder.() -> Unit): Int {
        return delete(buildFilter(block))
    }

    suspend fun replace(block: FilterBuilder.() -> Unit, document: BsonDocument, upsert: Boolean = false): Boolean {
        return replace(buildFilter(block), document, upsert)
    }

    suspend fun update(block: SelectiveUpdateBuilder.() -> Unit): Int {
        val (filter,update) = SelectiveUpdateBuilder(kodein).apply { block() }.build()
        return update(filter, update)
    }

    suspend fun updateOne(block: SelectiveUpdateBuilder.() -> Unit): Boolean {
        val (filter,update) = SelectiveUpdateBuilder(kodein).apply { block() }.build()
        return updateOne(filter, update)
    }

}

