package dev.helight.kodein

import dev.helight.kodein.collection.FindOptions
import dev.helight.kodein.collection.KPageCursor
import dev.helight.kodein.dsl.buildDocument
import dev.helight.kodein.dsl.buildFilter
import dev.helight.kodein.dsl.buildUpdate
import kotlinx.coroutines.flow.count
import kotlinx.coroutines.flow.firstOrNull
import kotlin.test.*
import kotlin.time.Clock

interface DocumentDatabaseContract : DocumentDatabaseScope, CollectionFindTests, StrictnessTests,
    CollectionArrayTests, TypedCollectionTests {

    @Test
    fun `Simple CRUD Operations`() = databaseScope {
        val collection = getCollection("test_collection")

        val insertResult = collection.insert(buildDocument {
            "name" put "Alice"
            "age" put 30
        })
        assertTrue(insertResult)

        val retrieved = collection.findOne { "name" eq "Alice" }
        assertNotNull(retrieved)
        assertEquals(30, retrieved.getInt("age"))

        val updateResult = collection.updateOne {
            where { "name" eq "Alice" }
            "name" set "Alice"
            "age" set 31
        }
        assertTrue(updateResult)

        val updated = collection.findOne { "name" eq "Alice" }
        assertNotNull(updated)
        assertEquals(31, updated.getInt("age"))

        val deleteResult = collection.deleteOne { "name" eq "Alice" }
        assertTrue(deleteResult)
        val deleted = collection.findOne { "name" eq "Alice" }
        assertEquals(null, deleted)
    }


    @Test
    fun `Simple logical Operations`() = databaseScope {
        val collection = getCollection("logic_test_collection")

        collection.insert(buildDocument {
            "name" put "Bob"
            "age" put 25
        })

        val andTrue = collection.find {
            "age" gt 20
            "age" lt 30
        }.count() > 0
        assertTrue(andTrue)

        val andFalse = collection.find {
            "age" gt 20
            "age" lt 24
        }.count() > 0
        assertFalse(andFalse)

        val orTrue = collection.find {
            or {
                "age" eq 15
                "age" eq 25
            }
        }.count() > 0
        assertTrue(orTrue)

        val orFalse = collection.find {
            or {
                "age" eq 15
                "age" eq 35
            }
        }.count() > 0
        assertFalse(orFalse)

        val notTrue = collection.find {
            not { "age" eq 30 }
        }.count() > 0
        assertTrue(notTrue)

        val notFalse = collection.find {
            not { "age" eq 25 }
        }.count() > 0
        assertFalse(notFalse)
    }

    @Test
    fun `Collection Management`() = databaseScope {
        val collectionName = "management_test_collection"

        val createdCollection = createCollection(collectionName)
        assertNotNull(createdCollection)

        val collections = listCollections()
        assertTrue(collections.contains(collectionName))

        val dropResult = dropCollection(collectionName)
        assertTrue(dropResult)

        val collectionsAfterDrop = listCollections()
        assertFalse(collectionsAfterDrop.contains(collectionName))
    }

    @Test
    fun `Test simple projection`() = databaseScope {
        val collection = getCollection("projection_test_collection")
        if (!collection.supportsProjections) {
            println("Collection does not support projections. Skipping test.")
            return@databaseScope
        }

        collection.insert(buildDocument {
            "name" put "Charlie"
            "age" put 28
            "city" put "New York"
            "details" put {
                "hobby" put "Photography"
                "profession" put "Engineer"
            }
        })

        // Find List
        val projected = collection.find {
            where { "name" eq "Charlie" }
            fields("name", "details.hobby")
        }.firstOrNull()

        println(projected?.bson)
        assertNotNull(projected)
        assertEquals("Charlie", projected.getString("name"))
        assertEquals("Photography", projected.getString("details.hobby"))
        assertNull(projected.getString("age"))
        assertNull(projected.getString("city"))
        assertNull(projected.getString("details.profession"))

        // Find One
        val projectedOne = collection.findOne {
            where { "name" eq "Charlie" }
            fields("age", "details.profession")
        }
        println(projectedOne?.bson)
        assertNotNull(projectedOne)
        assertEquals(28, projectedOne.getInt("age"))
        assertEquals("Engineer", projectedOne.getString("details.profession"))
        assertNull(projectedOne.getString("name"))
        assertNull(projectedOne.getString("city"))
        assertNull(projectedOne.getString("details.hobby"))
    }

    @Test
    fun `Test all bson datatypes`() = databaseScope {
        val collection = getCollection("datatype_test_collection")

        val insertResult = collection.insert(buildDocument {
            "stringField" put "Test String"
            "intField" put 42
            "longField" put 1234567890123L
            "doubleField" put 3.14159
            "booleanField" put true
            "nullField" put null
            "instantField" put Clock.System.now().truncatedToMillis()
            "arrayField" putArray {
                add(1)
                add(2)
                add(3)
            }
            "objectField" put buildDocument {
                "nestedString" put "Nested"
                "nestedInt" put 7
            }
        })
        assertTrue(insertResult)

        val retrieved = collection.find().firstOrNull()
        assertNotNull(retrieved)
        assertEquals("Test String", retrieved.getString("stringField"))
        assertEquals(42, retrieved.getInt("intField"))
        assertEquals(1234567890123L, retrieved.getLong("longField"))
        assertEquals(3.14159, retrieved.getDouble("doubleField"))
        assertEquals(true, retrieved.getBoolean("booleanField"))
        assertEquals(null, retrieved.getString("nullField"))
        assertNotNull(retrieved.getDateTime("instantField"))
        val arrayField = retrieved.get("arrayField")?.asArray()?.map { it.asInt32().value }?.toList()
        assertContentEquals(listOf(1, 2, 3), arrayField)
        assertEquals("Nested", retrieved.getString("objectField.nestedString"))
        assertEquals(7, retrieved.getInt("objectField.nestedInt"))
    }

    @Test
    fun `Pagination examples`() = databaseScope {
        val collection = getCollection("pagination_test_collection")

        for (i in 1..15) {
            collection.insert(buildDocument {
                "index" put i
            })
        }

        val pageSize = 5
        val page1 = collection.findPaginated(
            cursor = KPageCursor(1, pageSize),
            options = FindOptions().sortByAsc("index")
        )
        assertEquals(pageSize, page1.items.count())
        assertEquals(3, page1.pageCount)
        assertEquals(1, page1.page)
        val first = page1.items.firstOrNull()
        assertNotNull(first)
        assertEquals(1, first.getInt("index"))

        val page2 = collection.findPaginated(
            cursor = KPageCursor(2, pageSize),
            options = FindOptions().sortByAsc("index")
        )
        assertEquals(pageSize, page2.items.count())
        assertEquals(3, page2.pageCount)
        assertEquals(2, page2.page)
        val firstPage2 = page2.items.firstOrNull()
        assertNotNull(firstPage2)
        assertEquals(6, firstPage2.getInt("index"))

    }


    @Test
    fun `Nested field updates`() = databaseScope {
        val collection = getCollection("nested_update_test_collection")
        collection.insert(buildDocument {
            "name" put "Diana"
            "address" put buildDocument {
                "street" put "123 Main St"
                "city" put "Metropolis"
                "zip" put "12345"
            }
        })

        collection.updateOne(
            filter = buildFilter { "name" eq "Diana" },
            update = buildUpdate {
                "address.city" set "Gotham"
                "address.zip" set "54321"
            }
        )

        val updated = collection.findOne { "name" eq "Diana" }
        assertNotNull(updated)
        assertEquals("Gotham", updated.getString("address.city"))
        assertEquals("54321", updated.getString("address.zip"))
        assertEquals("123 Main St", updated.getString("address.street"))
    }

    @Test
    fun `Find by nested field`() = databaseScope {
        val collection = getCollection("nested_find_test_collection")
        collection.insert(buildDocument {
            "name" put "Ethan"
            "profile" put buildDocument {
                "twitter" put "@ethan_adventures"
                "instagram" put "@ethan_insta"
            }
        })
        val found = collection.findOne {
            "profile.twitter" eq "@ethan_adventures"
        }
        assertNotNull(found)
        assertEquals("Ethan", found.getString("name"))
    }

    @Test
    fun `Test unset and increment operations`() = databaseScope {
        val collection = getCollection("unset_increment_test_collection")
        collection.insert(buildDocument {
            "name" put "Fiona"
            "score" put 10
            "tempField" put "to be removed"
        })

        collection.updateOne(
            filter = buildFilter { "name" eq "Fiona" },
            update = buildUpdate {
                "score" inc 5
                -"tempField"
            }
        )

        val updated = collection.findOne { "name" eq "Fiona" }
        assertNotNull(updated)
        assertEquals(15, updated.getInt("score"))
        assertNull(updated.getString("tempField"))
    }

    @Test
    fun `Upsert with a updateOne`() = databaseScope {
        val collection = getCollection("upsert_test_collection")
        val update = buildUpdate {
            "visits" inc 1
            upsert = true
        }

        val upsertResult = collection.updateOne(
            filter = buildFilter { "username" eq "george" },
            update = update
        )
        assertTrue(upsertResult)

        val inserted = collection.findOne { "username" eq "george" }
        assertNotNull(inserted)
        assertEquals("george", inserted.getString("username"))
        assertEquals(1, inserted.getInt("visits"))

        val secondUpsertResult = collection.updateOne(
            filter = buildFilter { "username" eq "george" },
            update = update
        )
        assertTrue(secondUpsertResult)

        val updated = collection.findOne { "username" eq "george" }
        assertNotNull(updated)
        assertEquals(2, updated.getInt("visits"))
        assertEquals(inserted.bsonId, updated.bsonId)
    }

    @Test
    fun `Upsert with a update`() = databaseScope {
        val collection = getCollection("upsert_test_collection_bulk")
        val update = buildUpdate {
            "logins" inc 1
            upsert = true
        }
        val modifiedCount = collection.update(
            filter = buildFilter {
                "username" eq "harry"
                "secondary" eq 42
            },
            update = update
        )
        assertEquals(1, modifiedCount)
        val inserted = collection.findOne { "username" eq "harry" }
        assertNotNull(inserted)
        assertEquals("harry", inserted.getString("username"))
        assertEquals(42, inserted.getInt("secondary"))
        assertEquals(1, inserted.getInt("logins"))
    }

    @Test
    fun `Upsert with multiple equalities`() = databaseScope {
        val collection = getCollection("upsert_multiple_equalities_test_collection")
        val modifiedCount = collection.update(
            filter = buildFilter {
                "username" eq "isabel"
                "country" eq "Wonderland"
            },
            update = buildUpdate {
                "visits" inc 1
                upsert = true
            }
        )
        assertEquals(1, modifiedCount)
        val inserted = collection.findOne {
            "username" eq "isabel"
            "country" eq "Wonderland"
        }
        assertNotNull(inserted)
        assertEquals("isabel", inserted.getString("username"))
        assertEquals("Wonderland", inserted.getString("country"))
        assertEquals(1, inserted.getInt("visits"))
    }

    @Test
    fun `Usage of the raw id field`() = databaseScope {
        val collection = getCollection("raw_id_field_test_collection")
        collection.insert(buildDocument {
            "_id" put "custom_id_123"
            "data" put "Some data"
        })
        val retrieved = collection.findOne { "_id" eq "custom_id_123" }
        assertNotNull(retrieved)
        assertEquals("Some data", retrieved.getString("data"))
    }

    @Test
    fun `Replace with upsert`() = databaseScope {
        val collection = getCollection("replace_upsert_test_collection")
        val replaceDoc = buildDocument {
            "username" put "jack"
            "level" put 5
        }
        val replaceResult = collection.replace(
            filter = buildFilter {
                "username" eq "jack"
                "pinned" eq true
            },
            document = replaceDoc,
            upsert = true
        )
        assertTrue(replaceResult)
        val retrieved = collection.findOne { "username" eq "jack" }
        assertNotNull(retrieved)
        assertNull(retrieved.getString("pinned"))
        assertEquals(5, retrieved.getInt("level"))


        val replaceResult2 = collection.replace(
            filter = buildFilter {
                "_id" eq "my-custom-id"
            },
            document = buildDocument {
                "username" put "jack"
                "level" put 6
            },
            upsert = true
        )
        assertTrue(replaceResult2)
        val retrieved2 = collection.findOne { "_id" eq "my-custom-id" }
        assertNotNull(retrieved2)
        assertEquals("jack", retrieved2.getString("username"))
        assertEquals(6, retrieved2.getInt("level"))
    }

    @Test
    fun `Update one returning`() = databaseScope {
        val collection = getCollection("update_returning_test_collection")
        val insertResult = collection.updateOneReturning {
            where { "username" eq "jack" }
            "level" inc 10
            upsert = true
        }
        assertNotNull(insertResult)
        assertNotNull(insertResult.id)
        assertEquals("jack", insertResult.getString("username"))
        assertEquals(10, insertResult.getInt("level"))

        val updateResult = collection.updateOneReturning {
            where { "username" eq "jack" }
            "level" inc 5
        }
        assertNotNull(updateResult)
        assertEquals("jack", updateResult.getString("username"))
        assertEquals(15, updateResult.getInt("level"))
    }
}