package dev.helight.kodein

import dev.helight.kodein.dsl.buildDocument
import dev.helight.kodein.memory.MemoryDocumentCollection
import dev.helight.kodein.spec.FieldIndexType
import dev.helight.kodein.spec.IndexDefinition
import dev.helight.kodein.spec.IndexList
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class IndexingTest {

    private lateinit var kodein: Kodein
    private lateinit var collection: MemoryDocumentCollection

    @BeforeEach
    fun setup() {
        kodein = Kodein()
        collection = MemoryDocumentCollection(kodein)
    }

    @Test
    fun `test basic index creation`() = runBlocking {
        val indexList = IndexList(
            indices = listOf(
                IndexDefinition("name", "name_idx", FieldIndexType.INDEXED)
            ),
            textIndices = emptySet()
        )
        collection.setIndices(indexList)
        
        // Insert test documents
        collection.insert(buildDocument { "name" put "Alice"; "age" put 30 })
        collection.insert(buildDocument { "name" put "Bob"; "age" put 25 })
        collection.insert(buildDocument { "name" put "Charlie"; "age" put 35 })
        
        // Query using indexed field
        val results = collection.find { "name" eq "Bob" }.toList()
        assertEquals(1, results.size)
        assertEquals("Bob", results[0].getString("name"))
    }

    @Test
    fun `test unique index`() = runBlocking {
        val indexList = IndexList(
            indices = listOf(
                IndexDefinition("email", "email_idx", FieldIndexType.UNIQUE)
            ),
            textIndices = emptySet()
        )
        collection.setIndices(indexList)
        
        collection.insert(buildDocument { "email" put "alice@example.com"; "name" put "Alice" })
        collection.insert(buildDocument { "email" put "bob@example.com"; "name" put "Bob" })
        
        val results = collection.find { "email" eq "alice@example.com" }.toList()
        assertEquals(1, results.size)
        assertEquals("Alice", results[0].getString("name"))
    }

    @Test
    fun `test range query with index`() = runBlocking {
        val indexList = IndexList(
            indices = listOf(
                IndexDefinition("age", "age_idx", FieldIndexType.INDEXED)
            ),
            textIndices = emptySet()
        )
        collection.setIndices(indexList)
        
        collection.insert(buildDocument { "name" put "Alice"; "age" put 30 })
        collection.insert(buildDocument { "name" put "Bob"; "age" put 25 })
        collection.insert(buildDocument { "name" put "Charlie"; "age" put 35 })
        collection.insert(buildDocument { "name" put "David"; "age" put 40 })
        
        val results = collection.find { "age" gte 30 }.toList()
        assertEquals(3, results.size)
    }

    @Test
    fun `test index maintenance on update`() = runBlocking {
        val indexList = IndexList(
            indices = listOf(
                IndexDefinition("status", "status_idx", FieldIndexType.INDEXED)
            ),
            textIndices = emptySet()
        )
        collection.setIndices(indexList)
        
        collection.insert(buildDocument { "name" put "Task1"; "status" put "pending" })
        collection.insert(buildDocument { "name" put "Task2"; "status" put "pending" })
        
        // Update one document
        collection.updateOne {
            where { "name" eq "Task1" }
            "status" set "completed"
        }
        
        val pendingResults = collection.find { "status" eq "pending" }.toList()
        assertEquals(1, pendingResults.size)
        assertEquals("Task2", pendingResults[0].getString("name"))
        
        val completedResults = collection.find { "status" eq "completed" }.toList()
        assertEquals(1, completedResults.size)
        assertEquals("Task1", completedResults[0].getString("name"))
    }

    @Test
    fun `test index maintenance on delete`() = runBlocking {
        val indexList = IndexList(
            indices = listOf(
                IndexDefinition("category", "category_idx", FieldIndexType.INDEXED)
            ),
            textIndices = emptySet()
        )
        collection.setIndices(indexList)
        
        collection.insert(buildDocument { "name" put "Item1"; "category" put "A" })
        collection.insert(buildDocument { "name" put "Item2"; "category" put "A" })
        collection.insert(buildDocument { "name" put "Item3"; "category" put "B" })
        
        collection.deleteOne { "name" eq "Item1" }
        
        val results = collection.find { "category" eq "A" }.toList()
        assertEquals(1, results.size)
        assertEquals("Item2", results[0].getString("name"))
    }

    @Test
    fun `test multiple indices`() = runBlocking {
        val indexList = IndexList(
            indices = listOf(
                IndexDefinition("name", "name_idx", FieldIndexType.INDEXED),
                IndexDefinition("age", "age_idx", FieldIndexType.INDEXED)
            ),
            textIndices = emptySet()
        )
        collection.setIndices(indexList)
        
        collection.insert(buildDocument { "name" put "Alice"; "age" put 30 })
        collection.insert(buildDocument { "name" put "Bob"; "age" put 25 })
        collection.insert(buildDocument { "name" put "Charlie"; "age" put 30 })
        
        // Query using one index
        val nameResults = collection.find { "name" eq "Alice" }.toList()
        assertEquals(1, nameResults.size)
        
        // Query using another index
        val ageResults = collection.find { "age" eq 30 }.toList()
        assertEquals(2, ageResults.size)
    }

    @Test
    fun `test In filter with index`() = runBlocking {
        val indexList = IndexList(
            indices = listOf(
                IndexDefinition("category", "category_idx", FieldIndexType.INDEXED)
            ),
            textIndices = emptySet()
        )
        collection.setIndices(indexList)
        
        collection.insert(buildDocument { "name" put "Item1"; "category" put "A" })
        collection.insert(buildDocument { "name" put "Item2"; "category" put "B" })
        collection.insert(buildDocument { "name" put "Item3"; "category" put "C" })
        collection.insert(buildDocument { "name" put "Item4"; "category" put "D" })
        
        val results = collection.find { "category" inList listOf("A", "C") }.toList()
        assertEquals(2, results.size)
        assertTrue(results.any { it.getString("name") == "Item1" })
        assertTrue(results.any { it.getString("name") == "Item3" })
    }
}
