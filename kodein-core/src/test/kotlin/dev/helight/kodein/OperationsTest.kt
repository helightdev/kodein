package dev.helight.kodein

import org.bson.BsonBoolean
import org.bson.BsonDateTime
import org.bson.BsonDocument
import org.bson.BsonDouble
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonString
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class OperationsTest {


    @Test
    fun `putEmbedded and getEmbedded test cases`() {
        val d0 = BsonDocument()
        d0.putEmbedded("a.b.c", BsonInt32(42))
        assertEquals(BsonInt32(42), d0.getEmbedded("a.b.c"))
        assertEquals(BsonInt32(42), d0["a"]!!.asDocument()["b"]!!.asDocument()["c"])

        val d1 = BsonDocument()
        d1.putEmbedded("x.y", BsonInt32(100))
        d1.putEmbedded("x.z", BsonInt32(200))
        assertEquals(BsonInt32(100), d1.getEmbedded("x.y"))
        assertEquals(BsonInt32(200), d1.getEmbedded("x.z"))
        assertEquals(BsonInt32(100), d1["x"]!!.asDocument()["y"])
        assertEquals(BsonInt32(200), d1["x"]!!.asDocument()["z"])

        val d2 = BsonDocument()
        d2.putEmbedded("m", BsonInt32(1))
        assertEquals(BsonInt32(1), d2.getEmbedded("m"))
        assertEquals(BsonInt32(1), d2["m"])

        d2.putEmbedded("m.n.o", BsonInt32(2))
        assertEquals(BsonInt32(2), d2.getEmbedded("m.n.o"))
        assertEquals(BsonInt32(2), d2["m"]!!.asDocument()["n"]!!.asDocument()["o"])
    }

    @Test
    fun `unsetEmbedded test cases`() {
        val d0 = BsonDocument()
        d0.putEmbedded("a.b.c", BsonInt32(42))
        d0.unsetEmbedded("a.b.c")
        assertEquals(null, d0.getEmbedded("a.b.c"))
        assertEquals(0, d0["a"]!!.asDocument()["b"]!!.asDocument().size)

        val d1 = BsonDocument()
        d1.putEmbedded("x.y", BsonInt32(100))
        d1.putEmbedded("x.z", BsonInt32(200))
        d1.unsetEmbedded("x.y")
        assertEquals(null, d1.getEmbedded("x.y"))
        assertEquals(BsonInt32(200), d1.getEmbedded("x.z"))
        assertEquals(null, d1["x"]!!.asDocument()["y"])
        assertEquals(BsonInt32(200), d1["x"]!!.asDocument()["z"])
    }

    @Test
    fun `incEmbedded test cases`() {
        val d0 = BsonDocument()
        d0.putEmbedded("counter", BsonInt32(10))
        d0.incEmbedded("counter", 5)
        assertEquals(BsonInt32(15), d0.getEmbedded("counter"))

        val d1 = BsonDocument()
        d1.putEmbedded("nested.value", BsonInt32(20))
        d1.incEmbedded("nested.value", 10)
        assertEquals(BsonInt32(30), d1.getEmbedded("nested.value"))

        // Int64
        val d2 = BsonDocument()
        d2.putEmbedded("bigCounter", BsonInt64(100L))
        d2.incEmbedded("bigCounter", 50L)
        assertEquals(BsonInt64(150L), d2.getEmbedded("bigCounter"))

        // Double
        val d3 = BsonDocument()
        d3.putEmbedded("floatCounter", BsonDouble(1.5))
        d3.incEmbedded("floatCounter", 2.5)
        assertEquals(BsonDouble(4.0), d3.getEmbedded("floatCounter"))

        // Non-existing field
        val d4 = BsonDocument()
        d4.incEmbedded("newCounter", 7)
        assertEquals(BsonInt32(7), d4.getEmbedded("newCounter"))
    }

    @Test
    fun `compareBsonValues test cases`() {
        // Int32 comparisons
        assertEquals(0, compareBsonValues(BsonInt32(10), BsonInt32(10)))
        assertEquals(-1, compareBsonValues(BsonInt32(5), BsonInt32(10)))
        assertEquals(1, compareBsonValues(BsonInt32(15), BsonInt32(10)))
        assertEquals(-1, compareBsonValues(null, BsonInt32(10)))
        assertEquals(1, compareBsonValues(BsonInt32(10), null))
        assertEquals(0, compareBsonValues(null, null))

        /// Int64 comparisons
        assertEquals(0, compareBsonValues(BsonInt64(20L), BsonInt64(20L)))
        assertEquals(-1, compareBsonValues(BsonInt64(15L), BsonInt64(20L)))
        assertEquals(1, compareBsonValues(BsonInt64(25L), BsonInt64(20L)))

        // Double comparisons
        assertEquals(0, compareBsonValues(BsonDouble(3.14), BsonDouble(3.14)))
        assertEquals(-1, compareBsonValues(BsonDouble(2.71), BsonDouble(3.14)))
        assertEquals(1, compareBsonValues(BsonDouble(4.0), BsonDouble(3.14)))

        // Mixed type comparisons
        assertEquals(-1, compareBsonValues(BsonInt32(10), BsonInt64(20L)))
        assertEquals(1, compareBsonValues(BsonDouble(30.0), BsonInt32(20)))

        // Boolean comparisons
        assertEquals(0, compareBsonValues(BsonBoolean(true), BsonBoolean(true)))
        assertEquals(-1, compareBsonValues(BsonBoolean(false), BsonBoolean(true)))
        assertEquals(1, compareBsonValues(BsonBoolean(true), BsonBoolean(false)))

        /// String comparisons
        assertEquals(0, compareBsonValues(BsonString("abc"), BsonString("abc")))
        assertTrue { compareBsonValues(BsonString("abc"), BsonString("def")) < 0 }
        assertTrue { compareBsonValues(BsonString("def"), BsonString("abc")) > 0 }

        /// DateTime comparisons
        assertEquals(0, compareBsonValues(BsonDateTime(1000L), BsonDateTime(1000L)))
        assertEquals(-1, compareBsonValues(BsonDateTime(500L), BsonDateTime(1000L)))
        assertEquals(1, compareBsonValues(BsonDateTime(1500L), BsonDateTime(1000L)))

    }

}