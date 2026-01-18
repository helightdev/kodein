package dev.helight.kodein

import dev.helight.kodein.collection.DocumentDatabase
import dev.helight.kodein.dsl.buildDocument
import dev.helight.kodein.memory.MemoryDocumentDatabase
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import java.io.File
import java.nio.file.Files
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

    @Test
    fun `Test File Persistence`() = runBlocking {
        val tempDir = Files.createTempDirectory("kodein-test").toFile()
        val dbFile = File(tempDir, "test.db")
        val path = dbFile.absolutePath

        try {
            val kodein = Kodein()
            val db1 = MemoryDocumentDatabase(kodein, path)
            val col1 = db1.getCollection("test")
            col1.insert(buildDocument {
                "key" put "value"
            })
            db1.close()

            assertTrue(dbFile.exists(), "Database file should exist after close")

            val db2 = MemoryDocumentDatabase(kodein, path)
            db2.open()
            val col2 = db2.getCollection("test")
            val doc = col2.findOne { "key" eq "value" }
            assertNotNull(doc, "Document should be recovered from file")
            assertEquals("value", doc.getString("key"))
            db2.close()
        } finally {
            dbFile.delete()
            tempDir.delete()
        }
    }

    @Test
    fun `Test Complex File Persistence`() = runBlocking {
        val tempDir = Files.createTempDirectory("kodein-test-complex").toFile()
        val dbFile = File(tempDir, "complex.db")
        val path = dbFile.absolutePath

        try {
            val kodein = Kodein()
            val db1 = MemoryDocumentDatabase(kodein, path)
            
            // Multiple collections
            val users = db1.getCollection("users")
            val posts = db1.getCollection("posts")
            
            users.insert(buildDocument { "username" put "alice"; "id" put 1 })
            users.insert(buildDocument { "username" put "bob"; "id" put 2 })
            
            posts.insert(buildDocument { "title" put "Hello World"; "authorId" put 1 })
            posts.insert(buildDocument { "title" put "Kodein is cool"; "authorId" put 2 })
            
            db1.close()

            val db2 = MemoryDocumentDatabase(kodein, path)
            db2.open()
            
            assertEquals(setOf("users", "posts"), db2.listCollections())
            
            val users2 = db2.getCollection("users")
            assertEquals(2, users2.count {  })
            assertNotNull(users2.findOne { "username" eq "alice" })
            
            val posts2 = db2.getCollection("posts")
            assertEquals(2, posts2.count {  })
            assertNotNull(posts2.findOne { "title" eq "Hello World" })
            
            db2.close()
        } finally {
            dbFile.delete()
            tempDir.delete()
        }
    }

    @Test
    fun `Test Persistence when file does not exist`() = runBlocking {
        val tempDir = Files.createTempDirectory("kodein-test-not-exist").toFile()
        val dbFile = File(tempDir, "nonexistent.db")
        val path = dbFile.absolutePath

        try {
            val kodein = Kodein()
            val db = MemoryDocumentDatabase(kodein, path)
            
            // Should not throw and should be empty
            db.open()
            assertTrue(db.listCollections().isEmpty())
            
            db.getCollection("new_col").insert(buildDocument { "test" put true })
            db.close()
            
            assertTrue(dbFile.exists())
        } finally {
            dbFile.delete()
            tempDir.delete()
        }
    }

    @Test
    fun `Test open without close immediately`() = runBlocking {
        val tempDir = Files.createTempDirectory("kodein-test-open").toFile()
        val dbFile = File(tempDir, "open_test.db")
        val path = dbFile.absolutePath

        try {
            val kodein = Kodein()
            // Create a file first
            val db1 = MemoryDocumentDatabase(kodein, path)
            db1.getCollection("test").insert(buildDocument { "a" put 1 })
            db1.close()

            // Open it with a new instance
            val db2 = MemoryDocumentDatabase(kodein, path)
            db2.open()
            val doc = db2.getCollection("test").findOne { "a" eq 1 }
            assertNotNull(doc)
            
            // Make some changes but don't close yet
            db2.getCollection("test").insert(buildDocument { "a" put 2 })
            
            // Re-open with a third instance WITHOUT closing db2
            // db2 is still in memory, but db3 should load the OLD state from file
            val db3 = MemoryDocumentDatabase(kodein, path)
            db3.open()
            assertEquals(1, db3.getCollection("test").count {  })
            assertNotNull(db3.getCollection("test").findOne { "a" eq 1 })
            
            db2.close()
            db3.close()
        } finally {
            dbFile.delete()
            tempDir.delete()
        }
    }

}