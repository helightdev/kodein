package dev.helight.kodein.mongo

import com.mongodb.client.model.InsertManyOptions
import com.mongodb.client.model.ReplaceOptions
import com.mongodb.client.model.UpdateOptions
import com.mongodb.kotlin.client.coroutine.MongoCollection
import dev.helight.kodein.KDocument
import dev.helight.kodein.Kodein
import dev.helight.kodein.collection.DocumentCollection
import dev.helight.kodein.collection.Filter
import dev.helight.kodein.collection.FindOptions
import dev.helight.kodein.collection.Update
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.map
import org.bson.BsonDocument

class MongoDocumentCollection(
    val collection: MongoCollection<BsonDocument>, override val kodein: Kodein,
    private val relaxed : Boolean = true
) : DocumentCollection {
    override val supportsProjections: Boolean
        get() = true
    
    override suspend fun insert(document: BsonDocument): Boolean {
        val result = collection.insertOne(document)
        return result.insertedId != null
    }

    override suspend fun insert(documents: List<BsonDocument>): Int {
        val result = collection.insertMany(documents)
        return if (result.wasAcknowledged()) result.insertedIds.size else 0
    }

    override suspend fun update(
        filter: Filter,
        update: Update
    ): Int {
        val bsonFilter = MongoFilterConverter.convert(filter, relaxed)
        val result = collection.updateMany(
            bsonFilter, MongoFilterConverter.convertUpdates(update), UpdateOptions().upsert(update.upsert)
        )
        return if (result.upsertedId != null) 1 else result.modifiedCount.toInt()
    }

    override suspend fun updateOne(
        filter: Filter,
        update: Update
    ): Boolean {
        val bsonFilter = MongoFilterConverter.convert(filter, relaxed)
        val result = collection.updateOne(
            bsonFilter, MongoFilterConverter.convertUpdates(update), UpdateOptions().upsert(update.upsert)
        )
        return result.modifiedCount > 0 || result.upsertedId != null
    }

    override suspend fun replace(
        filter: Filter,
        document: BsonDocument,
        upsert: Boolean
    ): Boolean {
        val bsonFilter = MongoFilterConverter.convert(filter, relaxed)
        val result = collection.replaceOne(bsonFilter, document, ReplaceOptions().upsert(upsert))
        return result.modifiedCount > 0 || result.upsertedId != null
    }

    override suspend fun delete(filter: Filter): Int {
        val bsonFilter = MongoFilterConverter.convert(filter, relaxed)
        val result = collection.deleteMany(bsonFilter)
        return result.deletedCount.toInt()
    }

    override suspend fun deleteOne(filter: Filter): Boolean {
        val bsonFilter = MongoFilterConverter.convert(filter, relaxed)
        val result = collection.deleteOne(bsonFilter)
        return result.deletedCount > 0
    }

    override suspend fun count(filter: Filter?): Long {
        if (filter == null) return collection.countDocuments()
        val bsonFilter = MongoFilterConverter.convert(filter, relaxed)
        return collection.countDocuments(bsonFilter)
    }

    override suspend fun find(
        filter: Filter?,
        options: FindOptions
    ): Flow<KDocument> {
        val bsonFilter = filter?.let { MongoFilterConverter.convert(it, relaxed) } ?: BsonDocument()
        var findFlow = collection.find(bsonFilter)
        if (options.fields.isNotEmpty()) {
            findFlow = findFlow.projection(MongoFilterConverter.projection(options.fields))
        }
        findFlow = findFlow.skip(options.skip)
            .limit(options.limit)
            .sort(MongoFilterConverter.convertSort(options.sort))
        return findFlow.map { kodein.introspect(it) }
    }

    override suspend fun findOne(filter: Filter, options: FindOptions): KDocument? {
        val bsonFilter = MongoFilterConverter.convert(filter, relaxed)
        var findFlow = collection.find(bsonFilter)
        if (options.fields.isNotEmpty()) {
            findFlow = findFlow.projection(MongoFilterConverter.projection(options.fields))
        }
        return findFlow.firstOrNull()
            ?.let { kodein.introspect(it) }
    }

}