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
    fun `Basic array operations`() = databaseScope {
        val collection = getCollection("array_element_contains")
        val insertCount = collection.insert(inventory)
        assertEquals(5, insertCount)

        val exactMatch = collection.find {
            "tags" eq listOf("red", "blank")
        }.toList()
        assertEquals(1, exactMatch.size)
        assertEquals("notebook", exactMatch[0].getString("item"))

        val notMatch = collection.find {
            "tags" notEq listOf("red", "blank")
        }.toList()
        assertEquals(4, notMatch.size)

        val elementContains = collection.find {
            "tags" contains "red"
        }.toList()
        assertEquals(4, elementContains.size)

        val notContains = collection.find {
            "tags" notContains "red"
        }.toList()
        assertEquals(1, notContains.size)

        val intersects = collection.find {
            "tags" intersects listOf("red", "blank")
        }.toList()
        assertEquals(4, intersects.size)

        val notIntersects = collection.find {
            "tags" notIntersects listOf("red", "blank")
        }.toList()
        assertEquals(1, notIntersects.size)

        val equalsSet = collection.find {
            "tags" equalsSet listOf("red", "blank")
        }.toList()
        assertEquals(3, equalsSet.size)

        val containsAll = collection.find {
            "tags" containsAll listOf("red", "blank")
        }.toList()
        assertEquals(4, containsAll.size)
    }

}


