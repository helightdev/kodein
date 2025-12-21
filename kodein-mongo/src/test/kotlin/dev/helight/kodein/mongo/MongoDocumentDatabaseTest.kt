package dev.helight.kodein.mongo

import com.mongodb.kotlin.client.coroutine.MongoClient
import dev.helight.kodein.DocumentDatabaseContract
import dev.helight.kodein.Kodein
import dev.helight.kodein.collection.DocumentDatabase
import kotlinx.coroutines.runBlocking
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.Duration

@Testcontainers
class MongoDocumentDatabaseTest : DocumentDatabaseContract {
    companion object {

        @Container
        private val mongo = GenericContainer("mongo:latest")
            .withExposedPorts(27017)
            .withEnv("MONGO_INITDB_ROOT_USERNAME", "root")
            .withEnv("MONGO_INITDB_ROOT_PASSWORD", "example")
            .withStartupTimeout(Duration.ofSeconds(60))

        private val connectionString get() = "mongodb://root:example@localhost:${mongo.getMappedPort(27017)}/"
    }

    override fun databaseScope(block: suspend DocumentDatabase.() -> Unit) = runBlocking {
        val client = MongoClient.create(connectionString)
        val database = client.getDatabase("test")
        val provider = MongoDocumentDatabase(client, database, Kodein())
        block(provider)
        provider.close()
    }
}