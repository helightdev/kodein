package dev.helight.kodein

import dev.helight.kodein.collection.Filter
import dev.helight.kodein.dsl.buildDocument
import dev.helight.kodein.memory.MemoryDocumentCollection
import dev.helight.kodein.spec.IndexList
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.bson.BsonString
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class TextIndexTest {

    private lateinit var kodein: Kodein
    private lateinit var collection: MemoryDocumentCollection

    @BeforeEach
    fun setup() {
        kodein = Kodein()
        collection = MemoryDocumentCollection(kodein)
    }

    @Test
    fun `test text filter basic functionality`() = runBlocking {
        collection.insert(buildDocument { "title" put "Hello World"; "content" put "This is a test" })
        collection.insert(buildDocument { "title" put "Goodbye World"; "content" put "Another test" })
        collection.insert(buildDocument { "title" put "Test Document"; "content" put "Hello everyone" })
        
        // Test text filter without index (full scan)
        val filter = Filter.Text(BsonString("world"))
        val results = collection.find(filter).toList()
        
        assertEquals(2, results.size)
        assertTrue(results.any { it.getString("title") == "Hello World" })
        assertTrue(results.any { it.getString("title") == "Goodbye World" })
    }

    @Test
    fun `test text filter case insensitive`() = runBlocking {
        collection.insert(buildDocument { "description" put "UPPERCASE TEXT" })
        collection.insert(buildDocument { "description" put "lowercase text" })
        collection.insert(buildDocument { "description" put "MiXeD CaSe TeXt" })
        
        val filter = Filter.Text(BsonString("text"))
        val results = collection.find(filter).toList()
        
        assertEquals(3, results.size)
    }

    @Test
    fun `test text index for faster search`() = runBlocking {
        val indexList = IndexList(
            indices = emptyList(),
            textIndices = setOf("content")
        )
        collection.setIndices(indexList)
        
        collection.insert(buildDocument { "title" put "Doc1"; "content" put "The quick brown fox" })
        collection.insert(buildDocument { "title" put "Doc2"; "content" put "jumps over the lazy dog" })
        collection.insert(buildDocument { "title" put "Doc3"; "content" put "quick thinking required" })
        collection.insert(buildDocument { "title" put "Doc4"; "content" put "something else entirely" })
        
        val filter = Filter.Text(BsonString("quick"))
        val results = collection.find(filter).toList()
        
        assertEquals(2, results.size)
        assertTrue(results.any { it.getString("title") == "Doc1" })
        assertTrue(results.any { it.getString("title") == "Doc3" })
    }

    @Test
    fun `test text index with multiple words`() = runBlocking {
        val indexList = IndexList(
            indices = emptyList(),
            textIndices = setOf("description")
        )
        collection.setIndices(indexList)
        
        collection.insert(buildDocument { "name" put "Product1"; "description" put "high quality product with excellent features" })
        collection.insert(buildDocument { "name" put "Product2"; "description" put "affordable pricing and good quality" })
        collection.insert(buildDocument { "name" put "Product3"; "description" put "premium features at competitive prices" })
        
        // Search for "quality"
        val filter = Filter.Text(BsonString("quality"))
        val results = collection.find(filter).toList()
        
        assertEquals(2, results.size)
        assertTrue(results.any { it.getString("name") == "Product1" })
        assertTrue(results.any { it.getString("name") == "Product2" })
    }

    @Test
    fun `test text filter with non-string field`() = runBlocking {
        collection.insert(buildDocument { "title" put "Document"; "value" put 123 })
        
        val filter = Filter.Text(BsonString("123"))
        val results = collection.find(filter).toList()
        
        // Should return empty as value is not a string
        assertEquals(0, results.size)
    }

    @Test
    fun `test text filter with missing field`() = runBlocking {
        collection.insert(buildDocument { "title" put "Document" })
        
        val filter = Filter.Text(BsonString("test"))
        val results = collection.find(filter).toList()
        
        // Should return empty as field doesn't exist
        assertEquals(0, results.size)
    }

    @Test
    fun `test text index maintenance on update`() = runBlocking {
        val indexList = IndexList(
            indices = emptyList(),
            textIndices = setOf("tags")
        )
        collection.setIndices(indexList)
        
        collection.insert(buildDocument { "id" put 1; "tags" put "kotlin programming" })
        collection.insert(buildDocument { "id" put 2; "tags" put "java development" })
        
        // Update document
        collection.updateOne {
            where { "id" eq 1 }
            "tags" set "kotlin development"
        }
        
        // Search for "development"
        val filter = Filter.Text(BsonString("development"))
        val results = collection.find(filter).toList()
        
        assertEquals(2, results.size)
        
        // Search for "programming" (should not find the updated document)
        val filter2 = Filter.Text(BsonString("programming"))
        val results2 = collection.find(filter2).toList()
        
        assertEquals(0, results2.size)
    }

    @Test
    fun `test text index maintenance on delete`() = runBlocking {
        val indexList = IndexList(
            indices = emptyList(),
            textIndices = setOf("description")
        )
        collection.setIndices(indexList)
        
        collection.insert(buildDocument { "id" put 1; "description" put "important document" })
        collection.insert(buildDocument { "id" put 2; "description" put "another important file" })
        
        collection.deleteOne { "id" eq 1 }
        
        val filter = Filter.Text(BsonString("important"))
        val results = collection.find(filter).toList()
        
        assertEquals(1, results.size)
        assertEquals(2, results[0].getInt("id"))
    }

    @Test
    fun `test partial match with text filter`() = runBlocking {
        val indexList = IndexList(
            indices = emptyList(),
            textIndices = setOf("content")
        )
        collection.setIndices(indexList)
        
        collection.insert(buildDocument { "content" put "categorization is important" })
        collection.insert(buildDocument { "content" put "category management" })
        collection.insert(buildDocument { "content" put "uncategorized items" })
        
        // Should match "category" within larger words
        val filter = Filter.Text(BsonString("category"))
        val results = collection.find(filter).toList()
        
        // Only exact token matches, not partial
        assertEquals(1, results.size)
    }

    @Test
    fun `test text index only searches indexed fields`() = runBlocking {
        val indexList = IndexList(
            indices = emptyList(),
            textIndices = setOf("description")  // Only description is indexed
        )
        collection.setIndices(indexList)
        
        // Insert documents with both indexed (description) and non-indexed (title) fields
        collection.insert(buildDocument { 
            "title" put "contains keyword in title"
            "description" put "this is the description"
        })
        collection.insert(buildDocument { 
            "title" put "no match here"
            "description" put "this has the keyword"
        })
        collection.insert(buildDocument { 
            "title" put "keyword in both"
            "description" put "keyword also here"
        })
        
        // Search for "keyword" - should only find docs where it's in the INDEXED field (description)
        val filter = Filter.Text(BsonString("keyword"))
        val results = collection.find(filter).toList()
        
        // Should only return the 2 documents where "keyword" is in the description field
        // NOT the first document where "keyword" is only in the title
        assertEquals(2, results.size)
        assertTrue(results.all { it.getString("description")?.contains("keyword") == true })
        assertFalse(results.any { 
            it.getString("description") == "this is the description" && 
            it.getString("title")?.contains("keyword") == true
        })
    }
}
