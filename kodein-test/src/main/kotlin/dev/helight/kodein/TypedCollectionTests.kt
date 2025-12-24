package dev.helight.kodein

import dev.helight.kodein.dsl.crudScope
import dev.helight.kodein.spec.BaseDocument
import dev.helight.kodein.spec.TypedCollectionSpec
import dev.helight.kodein.spec.TypedEntitySpec
import kotlinx.coroutines.flow.toList
import kotlinx.serialization.Serializable
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

interface TypedCollectionTests : DocumentDatabaseScope {

    @Serializable
    data class Movie(
        val title: String,
        val rating: Double,
    ) : BaseDocument() {
        companion object Spec : TypedCollectionSpec<Movie>(Movie::class) {
            val title = field(Movie::title)
            val rating = field(Movie::rating)
        }
    }

    @Serializable
    data class Rating(
        val author: Author,
        val score: Double,
        val notes: String? = null,
    ) : BaseDocument() {
        companion object Spec : TypedCollectionSpec<Rating>(Rating::class) {
            val author = embeddedField(Rating::author, Author)
            val score = field(Rating::score)
        }
    }

    @Serializable
    data class Author(
        val name: String,
    ) {
        companion object Spec : TypedEntitySpec<Author>(Author::class) {
            val name = field(Author::name)
        }
    }

    @Serializable
    data class InventoryItem(
        val item: String,
        val qty: Int,
        val tags: Set<String>,
        val dimCm: List<Double>,
    ) : BaseDocument() {
        companion object Spec : TypedCollectionSpec<InventoryItem>(InventoryItem::class) {
            val item = field(InventoryItem::item)
            val qty = field(InventoryItem::qty)
            val tags = arrayField(InventoryItem::tags)
            val dimCm = arrayField(InventoryItem::dimCm)
        }
    }


    companion object {
        private val movies = listOf(
            Movie("Inception", 8.8),
            Movie("The Room", 3.7),
            Movie("Interstellar", 8.6),
            Movie("Cats", 2.8)
        )

        private val ratings = listOf(
            Rating(Author("Alice"), 9.0),
            Rating(Author("Bob"), 6.5, "Could be better"),
            Rating(Author("Charlie"), 4.0, "Not my taste")
        )

        private val inventory = listOf(
            InventoryItem("journal", 25, setOf("blank", "red"), listOf(14.0, 21.0)),
            InventoryItem("notebook", 50, setOf("red", "blank"), listOf(14.0, 21.0)),
            InventoryItem("paper", 100, setOf("red", "blank", "plain"), listOf(14.0, 21.0)),
            InventoryItem("planner", 75, setOf("blank", "red"), listOf(22.85, 30.0)),
            InventoryItem("postcard", 45, setOf("blue"), listOf(10.0, 15.25))
        )
    }

    @Test
    fun `Typed collection find and save`() = databaseScope {
        Movie.crudScope(getCollection("movies")) {
            it.insertMany(movies)

            val lowRatedMovies = it.find {
                rating lt 5.0
                sortAsc(Movie.rating)
            }.toList()
            assertEquals(2, lowRatedMovies.size)
            assertEquals("Cats", lowRatedMovies[0].title)
            assertEquals("The Room", lowRatedMovies[1].title)

            val highRatedMovies = it.find {
                rating gte 8.0
                limit(2)
                sortDesc(Movie.rating)
            }.toList()
            assertEquals(2, highRatedMovies.size)
            assertEquals("Inception", highRatedMovies[0].title)
            assertEquals("Interstellar", highRatedMovies[1].title)

            // Test modification and id retention
            val inceptionId = it.findOne {
                title eq "Inception"
            }?.id
            assertNotNull(inceptionId)

            val inception = it.findById(inceptionId)
            assertNotNull(inception)
            assertEquals("Inception", inception.title)

            val updated = it.save(inception) { copy(rating = 3.5) }
            assertEquals(updated.id, inception.id)

            val fetchedInception = it.findById(inceptionId)
            assertNotNull(fetchedInception)
            assertEquals(3.5, fetchedInception.rating)
        }
    }

    @Test
    fun `Typed collection updates and deletes`() = databaseScope {
        Movie.crudScope(getCollection("movies_updates_deletes")) {
            it.insertMany(movies)

            // Update all low rated movies to have a rating of 5.0
            val updatedCount = it.update {
                where { rating lt 6.0 }
                rating set 5.0
            }
            assertEquals(2, updatedCount)

            val midRatedMovies = it.find {
                rating eq 5.0
                sortAsc(Movie.title)
            }.toList()
            assertEquals(2, midRatedMovies.size)
            assertEquals("Cats", midRatedMovies[0].title)
            assertEquals("The Room", midRatedMovies[1].title)

            // Delete all movies with rating below 6.0
            it.deleteMany(midRatedMovies)
            val remainingMovies = it.find {
                sortAsc(Movie.title)
            }.toList()
            assertEquals(2, remainingMovies.size)
            assertEquals("Inception", remainingMovies[0].title)
            assertEquals("Interstellar", remainingMovies[1].title)
        }
    }

    @Test
    fun `Typed collection with embedded documents`() = databaseScope {
        Rating.crudScope(getCollection("ratings")) {
            it.insertMany(ratings)

            val highScores = it.find {
                score gte 7.0
                sortDesc(Rating.score)
            }.toList()
            assertEquals(1, highScores.size)
            assertEquals("Alice", highScores[0].author.name)
            assertEquals(9.0, highScores[0].score)

            val midScores = it.find {
                score inList listOf(4.0, 6.5)
                sortAsc(Rating.score)
            }.toList()
            assertEquals(2, midScores.size)
            assertEquals("Charlie", midScores[0].author.name)
            assertEquals("Bob", midScores[1].author.name)

            val byAlice = it.find {
                author.select { name } eq "Alice"
            }.toList()
            assertEquals(1, byAlice.size)
            assertEquals(9.0, byAlice[0].score)
        }
    }

    @Test
    fun `Typed collection array operations`() = databaseScope {
        InventoryItem.crudScope(getCollection("typed_inventory_items")) {
            it.insertMany(inventory)

            val dimQuery = it.find {
                dimCm contains 30.0
            }.toList()
            assertEquals(1, dimQuery.size)

            // Copied from array tests
            val exactMatch = it.find {
                tags eq listOf("red", "blank")
            }.toList()
            assertEquals(1, exactMatch.size)
            assertEquals("notebook", exactMatch[0].item)

            val notMatch = it.find {
                tags notEq listOf("red", "blank")
            }.toList()
            assertEquals(4, notMatch.size)

            val elementContains = it.find {
                tags contains "red"
            }.toList()
            assertEquals(4, elementContains.size)

            val notContains = it.find {
                tags notContains "red"
            }.toList()
            assertEquals(1, notContains.size)

            val intersects = it.find {
                tags intersects listOf("red", "blank")
            }.toList()
            assertEquals(4, intersects.size)

            val notIntersects = it.find {
                tags notIntersects listOf("red", "blank")
            }.toList()
            assertEquals(1, notIntersects.size)

            val equalsSet = it.find {
                tags equalsSet listOf("red", "blank")
            }.toList()
            assertEquals(3, equalsSet.size)

            val containsAll = it.find {
                tags containsAll listOf("red", "blank")
            }.toList()
            assertEquals(4, containsAll.size)

            val size = it.find {
                tags size 2
            }.toList()
            assertEquals(3, size.size)
        }
    }

    @Test
    fun `Update array and embedded fields`() = databaseScope {
        InventoryItem.crudScope(getCollection("typed_inventory_items_update")) {
            it.insertMany(inventory)

            val updatedCount = it.update {
                where { item eq "planner" }
                tags set listOf("updated", "tags")
                dimCm set listOf(99.9, 88.8)
            }
            assertEquals(1, updatedCount)

            val updatedItem = it.findOne {
                item eq "planner"
            }
            assertNotNull(updatedItem)
            assertEquals(setOf("updated", "tags"), updatedItem.tags)
            assertEquals(listOf(99.9, 88.8), updatedItem.dimCm)
        }

        Rating.crudScope(getCollection("ratings_update_embedded")) {
            it.insertMany(ratings)

            val updatedCount = it.update {
                where { author.select { name } eq "Bob" }
                author set Author("Robert")
            }
            assertEquals(1, updatedCount)

            val updatedRating = it.findOne {
                author.select { name } eq "Robert"
            }
            assertNotNull(updatedRating)
            assertEquals("Robert", updatedRating.author.name)
        }
    }

    @Test
    fun `Dsl based projections`() = databaseScope {
        Movie.crudScope(getCollection("movies_projections")) {
            it.insertMany(movies)
            val titlesOnly = it.untyped.find {
                sortAsc(Movie.title)
                fields(Movie, rating)
            }.toList()
            assertEquals(4, titlesOnly.size)
            for (doc in titlesOnly) {
                assertNotNull(doc.bsonId)
                assertNotNull(doc.get("title"))
                assertNull(doc.get("rating"))
            }
        }
    }

}