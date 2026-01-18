package dev.helight.kodein

import dev.helight.kodein.dsl.buildDocument
import kotlinx.coroutines.flow.toList
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import java.util.regex.Pattern

interface RegexFilterTests : DocumentDatabaseScope {

    @Test
    fun `Regex basic match`() = databaseScope {
        val collection = getCollection("regex_basic")
        val docs = listOf(
            buildDocument { "title" put "Inception" },
            buildDocument { "title" put "Interstellar" },
            buildDocument { "title" put "The Room" }
        )
        collection.insert(docs)

        val results = collection.find { "title" regex "Inter.*" }.toList()
        assertEquals(1, results.count())
        val first = results.firstOrNull()
        assertNotNull(first)
        assertEquals("Interstellar", first.getString("title"))
    }

    @Test
    fun `Regex case insensitive with options string`() = databaseScope {
        val collection = getCollection("regex_case_insensitive")
        val docs = listOf(
            buildDocument { "title" put "hello" },
            buildDocument { "title" put "HELLO" },
            buildDocument { "title" put "HeLLo" }
        )
        collection.insert(docs)

        val results = collection.find {
            // use options string to enable case-insensitive matching
            "title".regex("hello", "i")
        }.toList()

        assertEquals(3, results.count())
    }

    @Test
    fun `Regex with Pattern and flags`() = databaseScope {
        val collection = getCollection("regex_pattern_flags")
        val docs = listOf(
            buildDocument { "title" put "hello" },
            buildDocument { "title" put "HELLO" }
        )
        collection.insert(docs)

        val results = collection.find {
            "title" regex Pattern.compile("hello", Pattern.CASE_INSENSITIVE)
        }.toList()

        assertEquals(2, results.count())
    }

    @Test
    fun `Regex invalid pattern returns no matches`() = databaseScope {
        val collection = getCollection("regex_invalid")
        val docs = listOf(
            buildDocument { "title" put "abc" },
            buildDocument { "title" put "(unmatched" }
        )
        collection.insert(docs)

        // invalid pattern should be caught and simply not match anything
        val results = try {
            collection.find { "title" regex "(" }.toList()
        } catch (e: Exception) {
            emptyList()
        }
        assertEquals(0, results.count())
    }

    @Test
    fun `Regex special characters literal match`() = databaseScope {
        val collection = getCollection("regex_literal")
        val docs = listOf(
            buildDocument { "title" put "version 1.2" },
            buildDocument { "title" put "version 12" }
        )
        collection.insert(docs)

        // match the literal dot between numbers
        val results = collection.find { "title" regex "1\\.2" }.toList()
        assertEquals(1, results.count())
        val first = results.firstOrNull()
        assertNotNull(first)
        assertEquals("version 1.2", first.getString("title"))
    }
}
