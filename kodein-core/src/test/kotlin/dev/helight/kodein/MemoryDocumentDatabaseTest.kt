package dev.helight.kodein

import dev.helight.kodein.collection.DocumentDatabase
import dev.helight.kodein.dsl.buildDocument
import dev.helight.kodein.memory.MemoryDocumentDatabase
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import kotlin.test.Test
import kotlin.test.assertNotNull

class MemoryDocumentDatabaseTest : DocumentDatabaseContract {

    override fun databaseScope(block: suspend DocumentDatabase.() -> Unit) = runBlocking {
        val provider = MemoryDocumentDatabase(Kodein())
        block(provider)
        provider.close()
    }

    @Test
    fun `Test Dump & Load`() = databaseScope {
        val collection = getCollection("dump_load_test")
        collection.insert(buildDocument {
            "name" put "Charlie"
            "age" put 28
            "dasd" put null
        })
        val bytes = (this as MemoryDocumentDatabase).dumpBytes()

        val newProvider = MemoryDocumentDatabase(Kodein())
        newProvider.loadBytes(bytes)
        val newCollection = newProvider.getCollection("dump_load_test")
        val retrieved = newCollection.findOne {
            "name" eq "Charlie"
        }
        assertNotNull(retrieved)
        assertEquals(28, retrieved.getInt("age"))
    }

}