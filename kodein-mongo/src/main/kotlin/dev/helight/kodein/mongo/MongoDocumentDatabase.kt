package dev.helight.kodein.mongo

import com.mongodb.MongoClientSettings
import com.mongodb.kotlin.client.coroutine.MongoClient
import com.mongodb.kotlin.client.coroutine.MongoDatabase
import dev.helight.kodein.Kodein
import dev.helight.kodein.collection.DocumentCollection
import dev.helight.kodein.collection.DocumentDatabase
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.toSet
import org.bson.BsonDocument
import org.bson.codecs.configuration.CodecRegistries

class MongoDocumentDatabase(
    val client: MongoClient, val database: MongoDatabase, override val kodein: Kodein,
    private val relaxed: Boolean = false
) : DocumentDatabase {

    private val mergedRegistries = CodecRegistries.fromRegistries(
        kodein.codecRegistry,
        MongoClientSettings.getDefaultCodecRegistry(),
    )

    override suspend fun getCollection(name: String): DocumentCollection {
        val collection = database.getCollection<BsonDocument>(name)
            .withCodecRegistry(mergedRegistries)
        return MongoDocumentCollection(collection, kodein, relaxed)
    }

    override suspend fun createCollection(name: String): DocumentCollection {
        database.createCollection(name)
        val collection = database.getCollection<BsonDocument>(name)
            .withCodecRegistry(mergedRegistries)
        return MongoDocumentCollection(collection, kodein, relaxed)
    }

    override suspend fun dropCollection(name: String): Boolean {
        val exists = database.listCollectionNames().firstOrNull { it == name } != null
        if (!exists) return false
        database.getCollection<BsonDocument>(name).drop()
        return true
    }

    override suspend fun listCollections(): Set<String> {
        return database.listCollectionNames().toSet()
    }

    override fun toString(): String {
        return "MongoDocumentDatabase()"
    }

    override fun close() {
        client.close()
    }
}

