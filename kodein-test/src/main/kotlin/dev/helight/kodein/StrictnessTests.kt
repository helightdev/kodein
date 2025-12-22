package dev.helight.kodein

import dev.helight.kodein.dsl.buildDocument
import kotlinx.coroutines.flow.toList
import kotlin.test.Test
import kotlin.test.assertEquals

interface StrictnessTests : DocumentDatabaseScope {

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
    fun `Scalars are not equal to arrays`() = databaseScope {
        if (!isStrict) {
            println("Skipping strictness test on non-strict database")
            return@databaseScope
        }

        val collection = getCollection("scalar_vs_array_equality")
        val insertCount = collection.insert(inventory)
        assertEquals(5, insertCount)

        val exactMatch = collection.find {
            "tags" eq "red"
        }.toList()
        assertEquals(0, exactMatch.size)

        val exactMatchSanity = collection.find {
            "item" eq listOf("journal")
        }.toList()
        assertEquals(0, exactMatchSanity.size)
    }

    @Test
    fun `In and Nin do not peform array intersections`() = databaseScope {
        if (!isStrict) {
            println("Skipping strictness test on non-strict database")
            return@databaseScope
        }

        val collection = getCollection("in_nin_array_intersection")
        val insertCount = collection.insert(inventory)
        assertEquals(5, insertCount)

        val inMatch = collection.find {
            "tags" inList listOf("red", "blue")
        }.toList()
        assertEquals(0, inMatch.size)

        val ninMatch = collection.find {
            "tags" notInList listOf("red", "blank")
        }.toList()
        assertEquals(5, ninMatch.size)
    }

    @Test
    fun `Number coercion in equality checks`() = databaseScope {
        if (!isStrict) {
            println("Skipping strictness test on non-strict database")
            return@databaseScope
        }

        val collection = getCollection("number_coercion_equality")
        val insertCount = collection.insert(inventory)
        assertEquals(5, insertCount)

        val intMatch = collection.find {
            "qty" eq 25
        }.toList()
        assertEquals(1, intMatch.size)

        val doubleMatch = collection.find {
            "qty" eq 25.0
        }.toList()
        assertEquals(1, doubleMatch.size)

        val longMatch = collection.find {
            "qty" eq 25L
        }.toList()
        assertEquals(1, longMatch.size)

        val stringNoMatch = collection.find {
            "qty" eq "25"
        }.toList()
        assertEquals(0, stringNoMatch.size)

        // Other matcher types
        val neqMatch = collection.find {
            "qty" notEq 25L
        }.toList()
        println(neqMatch)
        assertEquals(4, neqMatch.size)

        val neqDouble = collection.find {
            "qty" notEq 25.0
        }.toList()
        assertEquals(4, neqDouble.size)

        val inMatch = collection.find {
            "qty" inList listOf(25, 50.0, 75L)
        }.toList()
        assertEquals(3, inMatch.size)

        val ninMatch = collection.find {
            "qty" notInList listOf(25.0, 50L)
        }.toList()
        assertEquals(3, ninMatch.size)
    }
}


