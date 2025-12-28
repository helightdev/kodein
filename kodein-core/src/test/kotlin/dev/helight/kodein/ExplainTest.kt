package dev.helight.kodein

import dev.helight.kodein.collection.Filter
import dev.helight.kodein.dsl.buildDocument
import dev.helight.kodein.dsl.buildFilter
import dev.helight.kodein.memory.MemoryDocumentCollection
import dev.helight.kodein.spec.FieldIndexType
import dev.helight.kodein.spec.IndexDefinition
import dev.helight.kodein.spec.IndexList
import kotlinx.coroutines.runBlocking
import org.bson.BsonString
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class ExplainTest {

    private lateinit var kodein: Kodein
    private lateinit var collection: MemoryDocumentCollection

    @BeforeEach
    fun setup() {
        kodein = Kodein()
        collection = MemoryDocumentCollection(kodein)
    }

    @Test
    fun `test explain for full scan`() = runBlocking {
        // No indices configured
        repeat(10) { i ->
            collection.insert(buildDocument { "name" put "Item$i"; "value" put i })
        }
        
        val explanation = collection.explain(buildFilter { "name" eq "Item5" })
        
        assertEquals("FULL_SCAN", explanation.planType)
        assertFalse(explanation.optimized)
        assertTrue(explanation.indexesUsed.isEmpty())
        assertTrue(explanation.estimatedCost > 0)
        assertEquals(10.0, explanation.estimatedCost, 0.1)
    }

    @Test
    fun `test explain for index scan`() = runBlocking {
        val indexList = IndexList(
            indices = listOf(
                IndexDefinition("status", "status_idx", FieldIndexType.INDEXED)
            ),
            textIndices = emptySet()
        )
        collection.setIndices(indexList)
        
        repeat(50) { i ->
            collection.insert(buildDocument { 
                "name" put "Item$i"
                "status" put if (i % 2 == 0) "active" else "inactive"
            })
        }
        
        val explanation = collection.explain(buildFilter { "status" eq "active" })
        
        assertEquals("INDEX_SCAN", explanation.planType)
        assertTrue(explanation.optimized)
        assertEquals(listOf("status_idx"), explanation.indexesUsed)
        assertTrue(explanation.estimatedCost < 50.0) // Should be less than full scan
        
        // Check details
        val details = explanation.details
        assertTrue(details.containsKey("indexName"))
        assertEquals("status_idx", details["indexName"])
    }

    @Test
    fun `test explain for unique index`() = runBlocking {
        val indexList = IndexList(
            indices = listOf(
                IndexDefinition("userId", "user_id_idx", FieldIndexType.UNIQUE)
            ),
            textIndices = emptySet()
        )
        collection.setIndices(indexList)
        
        repeat(100) { i ->
            collection.insert(buildDocument { "userId" put i; "name" put "User$i" })
        }
        
        val explanation = collection.explain(buildFilter { "userId" eq 42 })
        
        assertEquals("INDEX_SCAN", explanation.planType)
        assertTrue(explanation.optimized)
        assertEquals(listOf("user_id_idx"), explanation.indexesUsed)
        
        // Unique index should have very low estimated cost
        assertTrue(explanation.estimatedCost < 2.0)
    }

    @Test
    fun `test explain for range query`() = runBlocking {
        val indexList = IndexList(
            indices = listOf(
                IndexDefinition("price", "price_idx", FieldIndexType.INDEXED)
            ),
            textIndices = emptySet()
        )
        collection.setIndices(indexList)
        
        repeat(100) { i ->
            collection.insert(buildDocument { "product" put "Item$i"; "price" put i })
        }
        
        val explanation = collection.explain(buildFilter { "price" gte 50 })
        
        assertEquals("INDEX_SCAN", explanation.planType)
        assertTrue(explanation.optimized)
        assertEquals(listOf("price_idx"), explanation.indexesUsed)
        assertTrue(explanation.details.containsKey("expectedResults"))
    }

    @Test
    fun `test explain for text index`() = runBlocking {
        val indexList = IndexList(
            indices = emptyList(),
            textIndices = setOf("description")
        )
        collection.setIndices(indexList)
        
        repeat(20) { i ->
            collection.insert(buildDocument { 
                "title" put "Document$i"
                "description" put "Sample text with content $i"
            })
        }
        
        val filter = Filter.Text(BsonString("content"))
        val explanation = collection.explain(filter)
        
        assertEquals("TEXT_INDEX_SCAN", explanation.planType)
        assertTrue(explanation.optimized)
        assertTrue(explanation.indexesUsed.contains("description"))
        assertTrue(explanation.details.containsKey("searchTerm"))
    }

    @Test
    fun `test explain for conjunctive query with partial optimization`() = runBlocking {
        val indexList = IndexList(
            indices = listOf(
                IndexDefinition("category", "category_idx", FieldIndexType.INDEXED)
            ),
            textIndices = emptySet()
        )
        collection.setIndices(indexList)
        
        repeat(30) { i ->
            collection.insert(buildDocument {
                "category" put "cat${i % 5}"
                "priority" put i % 3
                "active" put (i % 2 == 0)
            })
        }
        
        // Conjunctive query: indexed field + non-indexed fields
        val filter = Filter.And(listOf(
            Filter.Field.Eq("category", BsonString("cat1")),
            Filter.Field.Eq("active", org.bson.BsonBoolean.TRUE)
        ))
        
        val explanation = collection.explain(filter)
        
        assertEquals("INDEX_SCAN", explanation.planType)
        assertTrue(explanation.optimized)
        assertEquals(listOf("category_idx"), explanation.indexesUsed)
        
        // Should indicate there's a remaining filter
        val details = explanation.details
        assertTrue(details.containsKey("hasRemainingFilter"))
        assertEquals(true, details["hasRemainingFilter"])
    }

    @Test
    fun `test explain null filter`() = runBlocking {
        repeat(5) { i ->
            collection.insert(buildDocument { "value" put i })
        }
        
        val explanation = collection.explain(null)
        
        assertEquals("FULL_SCAN", explanation.planType)
        assertFalse(explanation.optimized)
        assertTrue(explanation.indexesUsed.isEmpty())
    }

    @Test
    fun `test explain shows cost comparison`() = runBlocking {
        val indexList = IndexList(
            indices = listOf(
                IndexDefinition("indexed_field", "indexed_idx", FieldIndexType.INDEXED)
            ),
            textIndices = emptySet()
        )
        collection.setIndices(indexList)
        
        repeat(100) { i ->
            collection.insert(buildDocument { 
                "indexed_field" put "value${i % 10}"
                "non_indexed_field" put "other${i % 10}"
            })
        }
        
        val indexedExplanation = collection.explain(buildFilter { "indexed_field" eq "value5" })
        val nonIndexedExplanation = collection.explain(buildFilter { "non_indexed_field" eq "other5" })
        
        // Index scan should have lower cost than full scan
        assertTrue(indexedExplanation.estimatedCost < nonIndexedExplanation.estimatedCost)
        assertTrue(indexedExplanation.optimized)
        assertFalse(nonIndexedExplanation.optimized)
    }

    @Test
    fun `test explain for Or filter`() = runBlocking {
        val indexList = IndexList(
            indices = listOf(
                IndexDefinition("status", "status_idx", FieldIndexType.INDEXED)
            ),
            textIndices = emptySet()
        )
        collection.setIndices(indexList)
        
        repeat(20) { i ->
            collection.insert(buildDocument { "status" put "status${i % 3}" })
        }
        
        // OR queries are not optimized in current implementation
        val filter = Filter.Or(listOf(
            Filter.Field.Eq("status", BsonString("status1")),
            Filter.Field.Eq("status", BsonString("status2"))
        ))
        
        val explanation = collection.explain(filter)
        
        // Should fall back to full scan for OR queries
        assertEquals("FULL_SCAN", explanation.planType)
    }
}
