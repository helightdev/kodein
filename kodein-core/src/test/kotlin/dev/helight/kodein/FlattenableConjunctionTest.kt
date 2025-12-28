package dev.helight.kodein

import dev.helight.kodein.collection.Filter
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

class FlattenableConjunctionTest {

    private lateinit var kodein: Kodein
    private lateinit var collection: MemoryDocumentCollection

    @BeforeEach
    fun setup() {
        kodein = Kodein()
        collection = MemoryDocumentCollection(kodein)
    }

    @Test
    fun `test optimizer flattens nested AND filters`() = runBlocking {
        val indexList = IndexList(
            indices = listOf(
                IndexDefinition("field1", "field1_idx", FieldIndexType.INDEXED),
                IndexDefinition("field2", "field2_idx", FieldIndexType.INDEXED),
                IndexDefinition("field3", "field3_idx", FieldIndexType.INDEXED)
            ),
            textIndices = emptySet()
        )
        collection.setIndices(indexList)
        
        // Create nested AND filters
        val innerAnd = Filter.And(listOf(
            Filter.Field.Eq("field1", BsonString("a")),
            Filter.Field.Eq("field2", BsonString("b"))
        ))
        val outerAnd = Filter.And(listOf(
            innerAnd,
            Filter.Field.Eq("field3", BsonString("c"))
        ))
        
        val explanation = collection.explain(outerAnd)
        
        // Should use an index, not fall back to full scan
        assertTrue(explanation.optimized, "Query should be optimized")
        assertNotEquals("FULL_SCAN", explanation.planType)
        assertFalse(explanation.indexesUsed.isEmpty(), "Should use at least one index")
    }

    @Test
    fun `test deeply nested AND filters are flattened`() = runBlocking {
        val indexList = IndexList(
            indices = listOf(
                IndexDefinition("a", "a_idx", FieldIndexType.UNIQUE),
                IndexDefinition("b", "b_idx", FieldIndexType.INDEXED),
                IndexDefinition("c", "c_idx", FieldIndexType.INDEXED),
                IndexDefinition("d", "d_idx", FieldIndexType.INDEXED)
            ),
            textIndices = emptySet()
        )
        collection.setIndices(indexList)
        
        // Create deeply nested structure: AND(AND(AND(a, b), c), d)
        val level1 = Filter.And(listOf(
            Filter.Field.Eq("a", BsonString("1")),
            Filter.Field.Eq("b", BsonString("2"))
        ))
        val level2 = Filter.And(listOf(
            level1,
            Filter.Field.Eq("c", BsonString("3"))
        ))
        val level3 = Filter.And(listOf(
            level2,
            Filter.Field.Eq("d", BsonString("4"))
        ))
        
        val explanation = collection.explain(level3)
        
        // Should recognize the unique index on 'a' and use it
        assertTrue(explanation.optimized)
        assertTrue(explanation.indexesUsed.contains("a_idx"), 
            "Should use the unique index (best selectivity)")
    }

    @Test
    fun `test mixed nested structures with OR are not flattened`() = runBlocking {
        val indexList = IndexList(
            indices = listOf(
                IndexDefinition("field1", "field1_idx", FieldIndexType.INDEXED)
            ),
            textIndices = emptySet()
        )
        collection.setIndices(indexList)
        
        // AND containing OR should not flatten the OR
        val orFilter = Filter.Or(listOf(
            Filter.Field.Eq("field1", BsonString("a")),
            Filter.Field.Eq("field1", BsonString("b"))
        ))
        val andFilter = Filter.And(listOf(
            orFilter,
            Filter.Field.Eq("field2", BsonString("c"))
        ))
        
        val explanation = collection.explain(andFilter)
        
        // OR cannot be optimized with current implementation, should fall back to full scan
        assertEquals("FULL_SCAN", explanation.planType)
    }
}
