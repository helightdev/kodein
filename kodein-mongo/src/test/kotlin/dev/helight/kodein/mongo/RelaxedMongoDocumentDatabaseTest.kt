package dev.helight.kodein.mongo

import com.mongodb.kotlin.client.coroutine.MongoClient
import dev.helight.kodein.DocumentDatabaseContract
import dev.helight.kodein.Kodein
import dev.helight.kodein.collection.DocumentDatabase
import dev.helight.kodein.dsl.buildDocument
import kotlinx.coroutines.runBlocking
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.Duration
import kotlin.test.Test

@Testcontainers
class RelaxedMongoDocumentDatabaseTest : DocumentDatabaseContract {
    companion object {

        @Container
        private val mongo = GenericContainer("mongo:latest")
            .withExposedPorts(27017)
            .withEnv("MONGO_INITDB_ROOT_USERNAME", "root")
            .withEnv("MONGO_INITDB_ROOT_PASSWORD", "example")
            .withStartupTimeout(Duration.ofSeconds(60))

        private val connectionString get() = "mongodb://root:example@localhost:${mongo.getMappedPort(27017)}/"
    }

    override val isStrict: Boolean
        get() = false

    override fun databaseScope(block: suspend DocumentDatabase.() -> Unit) = runBlocking {
        val client = MongoClient.create(connectionString)
        val database = client.getDatabase("test")
        val provider = MongoDocumentDatabase(client, database, Kodein(), true).withNamespace("relaxed")
        block(provider)
        provider.close()
    }

    @Test
    fun `Check if relaxed behavior is enabled`() = databaseScope {
        val collection = getCollection("relaxed_test")
        collection.insert(buildDocument {
            "tags" put listOf("Alice", "Bob")
        })

        val equality = collection.findOne {
            "tags" eq "Alice"
        }
        assert(equality != null)

        val inCheck = collection.findOne {
            "tags" inList listOf("Bob", "Charlie")
        }
        assert(inCheck != null)
    }
}