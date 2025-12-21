package dev.helight.kodein

import dev.helight.kodein.dsl.buildDocument
import kotlinx.coroutines.flow.toList
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

interface CollectionFindTests : DocumentDatabaseScope {

    companion object {
        private val movies = listOf(
            buildDocument { "title" put "Inception"; "rating" put 8.8 },
            buildDocument { "title" put "The Room"; "rating" put 3.7 },
            buildDocument { "title" put "Interstellar"; "rating" put 8.6 },
            buildDocument { "title" put "Cats"; "rating" put 2.8 }
        )
    }

    @Test
    fun `Find example 1`() = databaseScope {
        val collection = getCollection("find_example_1")
        val insertCount = collection.insert(movies)
        assertEquals(4, insertCount)

        val results = collection.find { "rating" lt 5; sortAsc("rating") }.toList()
        val count = results.count()
        assertEquals(2, count)

        val first = results.firstOrNull()
        assertNotNull(first)
        assertEquals("Cats", first.getString("title"))
        assertEquals(2.8, first.getDouble("rating"))

        val second = results.drop(1).firstOrNull()
        assertNotNull(second)
        assertEquals("The Room", second.getString("title"))
        assertEquals(3.7, second.getDouble("rating"))
    }

    @Test
    fun `Find example 2`() = databaseScope {
        val collection = getCollection("find_example_2")
        collection.insert(movies)
        val results = collection.find {
            "rating" gte 3.7
            limit(2).sortAsc("name")
        }.toList()
        val count = results.count()
        assertEquals(2, count)

        val first = results[0]
        assertNotNull(first)
        assertEquals("Inception", first.getString("title"))

        val second = results[1]
        assertNotNull(second)
        assertEquals("The Room", second.getString("title"))

    }
}


