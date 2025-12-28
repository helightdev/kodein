package dev.helight.kodein

import dev.helight.kodein.collection.Filter
import dev.helight.kodein.dsl.buildDocument
import dev.helight.kodein.dsl.buildFilter
import dev.helight.kodein.memory.MemoryDocumentCollection
import dev.helight.kodein.spec.FieldIndexType
import dev.helight.kodein.spec.IndexDefinition
import dev.helight.kodein.spec.IndexList
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.bson.BsonString
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class QueryOptimizerTest {

    private lateinit var kodein: Kodein
    private lateinit var collection: MemoryDocumentCollection

    @BeforeEach
    fun setup() {
        kodein = Kodein()
        collection = MemoryDocumentCollection(kodein)
    }

    @Test
    fun `test optimizer selects index for equality query`() = runBlocking {
        val indexList = IndexList(
            indices = listOf(
                IndexDefinition("name", "name_idx", FieldIndexType.INDEXED)
            ),
            textIndices = emptySet()
        )
        collection.setIndices(indexList)
        
        // Insert test data
        repeat(100) { i ->
            collection.insert(buildDocument { "name" put "Person$i"; "value" put i })
        }
        
        val explanation = collection.explain(buildFilter { "name" eq "Person50" })
        
        assertEquals("INDEX_SCAN", explanation.planType)
        assertTrue(explanation.optimized)
        assertEquals(listOf("name_idx"), explanation.indexesUsed)
    }

    @Test
    fun `test optimizer selects index for range query`() = runBlocking {
        val indexList = IndexList(
            indices = listOf(
                IndexDefinition("age", "age_idx", FieldIndexType.INDEXED)
            ),
            textIndices = emptySet()
        )
        collection.setIndices(indexList)
        
        repeat(50) { i ->
            collection.insert(buildDocument { "name" put "Person$i"; "age" put i })
        }
        
        val explanation = collection.explain(buildFilter { "age" gte 30 })
        
        assertEquals("INDEX_SCAN", explanation.planType)
        assertTrue(explanation.optimized)
        assertEquals(listOf("age_idx"), explanation.indexesUsed)
    }

    @Test
    fun `test optimizer handles non-indexed query`() = runBlocking {
        val indexList = IndexList(
            indices = listOf(
                IndexDefinition("name", "name_idx", FieldIndexType.INDEXED)
            ),
            textIndices = emptySet()
        )
        collection.setIndices(indexList)
        
        repeat(10) { i ->
            collection.insert(buildDocument { "name" put "Person$i"; "age" put i })
        }
        
        // Query on non-indexed field
        val explanation = collection.explain(buildFilter { "age" eq 5 })
        
        assertEquals("FULL_SCAN", explanation.planType)
        assertFalse(explanation.optimized)
        assertEquals(emptyList<String>(), explanation.indexesUsed)
    }

    @Test
    fun `test optimizer selects best index for conjunctive query`() = runBlocking {
        val indexList = IndexList(
            indices = listOf(
                IndexDefinition("email", "email_idx", FieldIndexType.UNIQUE),
                IndexDefinition("status", "status_idx", FieldIndexType.INDEXED)
            ),
            textIndices = emptySet()
        )
        collection.setIndices(indexList)
        
        repeat(20) { i ->
            collection.insert(buildDocument {
                "email" put "user$i@example.com"
                "status" put if (i % 2 == 0) "active" else "inactive"
            })
        }
        
        // Conjunctive query - should prefer unique index
        val filter = Filter.And(listOf(
            Filter.Field.Eq("email", BsonString("user5@example.com")),
            Filter.Field.Eq("status", BsonString("inactive"))
        ))
        
        val explanation = collection.explain(filter)
        
        assertEquals("INDEX_SCAN", explanation.planType)
        assertTrue(explanation.optimized)
        // Should use the unique index (email_idx) as it's more selective than status_idx
        assertEquals(listOf("email_idx"), explanation.indexesUsed)
    }

    @Test
    fun `test partial optimization of conjunctive query`() = runBlocking {
        val indexList = IndexList(
            indices = listOf(
                IndexDefinition("category", "category_idx", FieldIndexType.INDEXED)
            ),
            textIndices = emptySet()
        )
        collection.setIndices(indexList)
        
        repeat(30) { i ->
            collection.insert(buildDocument {
                "category" put "cat${i % 3}"
                "value" put i
                "flag" put (i % 2 == 0)
            })
        }
        
        // Mixed query: indexed + non-indexed fields
        val filter = Filter.And(listOf(
            Filter.Field.Eq("category", BsonString("cat1")),
            Filter.Field.Eq("flag", org.bson.BsonBoolean.TRUE)
        ))
        
        val explanation = collection.explain(filter)
        
        // Should use index for category, then filter on flag
        assertEquals("INDEX_SCAN", explanation.planType)
        assertTrue(explanation.optimized)
        assertEquals(listOf("category_idx"), explanation.indexesUsed)
        
        // Verify it still returns correct results
        val results = collection.find {
            "category" eq "cat1"
            "flag" eq true
        }.toList()
        
        assertTrue(results.all { it.getString("category") == "cat1" && it.getBoolean("flag") == true })
    }

    @Test
    fun `test text index optimization`() = runBlocking {
        val indexList = IndexList(
            indices = emptyList(),
            textIndices = setOf("description")
        )
        collection.setIndices(indexList)
        
        collection.insert(buildDocument { "title" put "Product1"; "description" put "Amazing product" })
        collection.insert(buildDocument { "title" put "Product2"; "description" put "Great quality" })
        collection.insert(buildDocument { "title" put "Product3"; "description" put "Amazing features" })
        
        val filter = Filter.Text(BsonString("amazing"))
        val explanation = collection.explain(filter)
        
        assertEquals("TEXT_INDEX_SCAN", explanation.planType)
        assertTrue(explanation.optimized)
        assertTrue(explanation.indexesUsed.contains("description"))
    }

    @Test
    fun `test cost estimation prefers unique index`() = runBlocking {
        val indexList = IndexList(
            indices = listOf(
                IndexDefinition("id", "id_idx", FieldIndexType.UNIQUE),
                IndexDefinition("category", "category_idx", FieldIndexType.INDEXED)
            ),
            textIndices = emptySet()
        )
        collection.setIndices(indexList)
        
        repeat(100) { i ->
            collection.insert(buildDocument {
                "id" put i
                "category" put "cat${i % 10}"
            })
        }
        
        val explanation1 = collection.explain(buildFilter { "id" eq 50 })
        val explanation2 = collection.explain(buildFilter { "category" eq "cat5" })
        
        // Unique index should have lower estimated cost
        assertTrue(explanation1.estimatedCost < explanation2.estimatedCost)
    }
}
