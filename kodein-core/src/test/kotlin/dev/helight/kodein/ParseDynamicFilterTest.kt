package dev.helight.kodein

import dev.helight.kodein.collection.Filter
import org.bson.BsonArray
import org.bson.BsonDateTime
import org.bson.BsonInt32
import org.bson.BsonObjectId
import org.bson.BsonString
import org.bson.types.ObjectId
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Instant
import kotlin.test.assertFailsWith

class ParseDynamicFilterTest {

    @Test
    fun `parseAll combines multiple filters with AND logic`() {
        val input = listOf(
            "field1:eq:\"value1\"",
            "field2:gt:42"
        )
        val parser = DynamicFilters(setOf("field1", "field2"))

        val result = parser.parseAll(input)

        assertTrue(result is Filter.And)
        val filters = (result as Filter.And).filters
        assertEquals(2, filters.size)
        assertTrue(filters[0] is Filter.Field.Eq)
        assertTrue(filters[1] is Filter.Field.Comp)
    }

    @Test
    fun `parseAll with single filter returns that filter`() {
        val input = listOf("field1:eq:\"value1\"")
        val parser = DynamicFilters(setOf("field1"))

        val result = parser.parseAll(input)

        assertTrue(result is Filter.Field.Eq)
        assertEquals("field1", (result as Filter.Field.Eq).path)
        assertEquals(BsonString("value1"), result.value)
    }

    @Test
    fun `parseSingle parses equality filter`() {
        val input = "field1:eq:\"value1\""
        val parser = DynamicFilters(setOf("field1"))

        val result = parser.parseSingle(input)

        assertTrue(result is Filter.Field.Eq)
        assertEquals("field1", (result as Filter.Field.Eq).path)
        assertEquals(BsonString("value1"), result.value)
    }

    @Test
    fun `parseSingle parses greater-than filter`() {
        val input = "field1:gt:42"
        val parser = DynamicFilters(setOf("field1"))

        val result = parser.parseSingle(input)

        assertTrue(result is Filter.Field.Comp)
        assertEquals("field1", (result as Filter.Field.Comp).path)
        assertEquals(BsonInt32(42), result.value)
        assertEquals(Filter.CompType.GT, result.type)
    }

    @Test
    fun `parseSingle parses 'in' operator with array value`() {
        val input = "field1:in:[\"value1\",\"value2\"]"
        val parser = DynamicFilters(setOf("field1"))

        val result = parser.parseSingle(input)

        assertTrue(result is Filter.Field.In)
        assertEquals("field1", (result as Filter.Field.In).path)
        val valueArray = result.value
        assertTrue(valueArray is BsonArray)
        assertEquals(2, valueArray.size)
        assertEquals(BsonString("value1"), valueArray[0])
        assertEquals(BsonString("value2"), valueArray[1])
    }

    @Test
    fun `parseSingle throws if filter uses an unpermitted field`() {
        val input = "field2:eq:\"value\""
        val parser = DynamicFilters(setOf("field1"))

        val exception = assertFailsWith<IllegalArgumentException> {
            parser.parseSingle(input)
        }

        assertEquals("Field 'field2' is not permitted in filters", exception.message)
    }

    @Test
    fun `parseSingle throws for 'in' operator with non-array value`() {
        val input = "field1:in:\"value\""
        val parser = DynamicFilters(setOf("field1"))

        val exception = assertFailsWith<IllegalArgumentException> {
            parser.parseSingle(input)
        }

        assertEquals("Value for 'in' operator must be a BsonArray", exception.message)
    }

    @Test
    fun `parseSingle throws for invalid filter format`() {
        val input = "invalid_filter_format"
        val parser = DynamicFilters(setOf("field1"))

        val exception = assertFailsWith<IllegalArgumentException> {
            parser.parseSingle(input)
        }

        assertEquals("Invalid filter format: invalid_filter_format", exception.message)
    }

    @Test
    fun `parseSingle throws for unsupported operator`() {
        val input = "field1:unsupported:\"value\""
        val parser = DynamicFilters(setOf("field1"))

        val exception = assertFailsWith<IllegalArgumentException> {
            parser.parseSingle(input)
        }

        assertEquals("Unsupported operator: unsupported", exception.message)
    }

    @Test
    fun `parseAll throws for an invalid filter in the list`() {
        val input = listOf(
            "field1:eq:\"value1\"",
            "invalid_filter"
        )
        val parser = DynamicFilters(setOf("field1"))

        val exception = assertFailsWith<IllegalArgumentException> {
            parser.parseAll(input)
        }

        assertEquals("Invalid filter format: invalid_filter", exception.message)
    }

    @Test
    fun `Dots in paths are allowed`() {
        val input = "user.name:eq:\"John\""
        val parser = DynamicFilters(setOf("user.name"))

        val result = parser.parseSingle(input)

        assertTrue(result is Filter.Field.Eq)
        assertEquals("user.name", (result as Filter.Field.Eq).path)
        assertEquals(BsonString("John"), result.value)
    }

    @Test
    fun `_id transformer works correctly`() {
        val id = ObjectId.get().toHexString()
        val filter = DynamicFilters.forCollection(TypedCollectionTests.Movie).parseSingle("id:eq:\"$id\"")

        assertTrue(filter is Filter.Field.Eq)
        assertEquals("_id", (filter as Filter.Field.Eq).path)
        assertEquals(ObjectId(id), (filter.value as BsonObjectId).value)
    }

    @Test
    fun `Iso times are parsed correctly`() {
        val input = "createdAt:eq:\"2024-01-01T12:00:00Z\""
        val parser = DynamicFilters(setOf("createdAt"))
        val result = parser.parseSingle(input)
        assertTrue(result is Filter.Field.Eq)
        assertEquals("createdAt", (result as Filter.Field.Eq).path)
        val instant = Instant.parse("2024-01-01T12:00:00Z")
        assertEquals(BsonDateTime(instant.toEpochMilli()), result.value)
    }
}