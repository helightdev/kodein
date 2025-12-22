package dev.helight.kodein

import dev.helight.kodein.dsl.buildDocument
import kotlinx.coroutines.flow.toList
import kotlin.test.Test
import kotlin.test.assertEquals

interface CollectionArrayTests : DocumentDatabaseScope {

    companion object {

        private val inventory = listOf(buildDocument {
            "item" put "journal"; "qty" put 25
            "tags" put listOf("blank", "red")
            "dim_cm" put listOf(14.0, 21.0)
        }, buildDocument {
            "item" put "notebook"; "qty" put 50
            "tags" put listOf("red", "blank")
            "dim_cm" put listOf(14.0, 21.0)
        }, buildDocument {
            "item" put "paper"; "qty" put 100
            "tags" put listOf("red", "blank", "plain")
            "dim_cm" put listOf(14.0, 21.0)
        }, buildDocument {
            "item" put "planner"; "qty" put 75
            "tags" put listOf("blank", "red")
            "dim_cm" put listOf(22.85, 30.0)
        }, buildDocument {
            "item" put "postcard"; "qty" put 45
            "tags" put listOf("blue")
            "dim_cm" put listOf(10.0, 15.25)
        })
    }

    @Test
    fun `Array Element Equality`() = databaseScope {
        val collection = getCollection("array_element_equality")
        val insertCount = collection.insert(inventory)
        assertEquals(5, insertCount)

        val exactMatch = collection.find {
            "tags" eq listOf("red", "blank")
        }.toList()
        assertEquals(1, exactMatch.size)
        assertEquals("notebook", exactMatch[0].getString("item"))

        val elementContains = collection.find {
            "tags" eq "red"
        }.toList()
        assertEquals(4, elementContains.size)

        val notEqualsExact = collection.find {
            "tags" notEq listOf("red", "blank")
        }.toList()
        assertEquals(4, notEqualsExact.size)

        val notEqualsElement = collection.find {
            "tags" notEq "red"
        }.toList()
        assertEquals(1, notEqualsElement.size)
    }

    @Test
    fun `Array Element Comparison`() = databaseScope {
        val collection = getCollection("array_element_comparison")
        val insertCount = collection.insert(inventory)
        assertEquals(5, insertCount)

        val greaterThan = collection.find {
            "dim_cm" gt 25.0
        }.toList()
        assertEquals(1, greaterThan.size)
        assertEquals("planner", greaterThan[0].getString("item"))

        val lessThanOrEqual = collection.find {
            "dim_cm" lte 15.25
        }.toList()
        assertEquals(4, lessThanOrEqual.size)
    }

    @Test
    fun `Array In and Not In`() = databaseScope {
        val collection = getCollection("array_in_and_not_in")
        val insertCount = collection.insert(inventory)
        assertEquals(5, insertCount)

        val inList = collection.find {
            "tags" inList listOf("blue", "plain")
        }.toList()
        assertEquals(2, inList.size)

        val notInList = collection.find {
            "tags" notInList listOf("blue", "plain")
        }.toList()
        assertEquals(3, notInList.size)
    }
}


