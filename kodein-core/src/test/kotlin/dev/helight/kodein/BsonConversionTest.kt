package dev.helight.kodein

import dev.helight.kodein.dsl.buildDocument
import dev.helight.kodein.serializers.KInstant
import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable
import org.bson.BsonDocument
import org.bson.json.JsonMode
import org.bson.json.JsonWriterSettings
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Clock
import kotlin.time.Instant

class BsonConversionTest {

    inline fun <reified T> recode(obj: T, mapper: Kodein = Kodein()): T {
        val bson = mapper.encode(obj, T::class.java)
        return mapper.decode(bson, T::class.java)
    }

    inline fun <reified T> recodeBinary(obj: T, mapper: Kodein = Kodein()): T {
        val bson = mapper.encodeBinary(obj, T::class.java)
        return mapper.decodeBinary(bson, T::class.java)
    }

    @Test
    fun `Verify basic value consistency`() {
        val testObj = TestClass("Test", 123, true)
        val instant = Clock.System.now().truncatedToMillis()

        assertEquals("Hello", recode("Hello"))
        assertEquals(42, recode(42))
        assertEquals(42L, recode(42L))
        assertEquals(3.14, recode(3.14))
        assertEquals(true, recode(true))
        assertEquals(false, recode(false))
        assertEquals(instant, recode(instant))
        assertEquals(null, recode<String?>(null))
        assertEquals(testObj, recode(testObj))

        assertEquals("Hello", recodeBinary("Hello"))
        assertEquals(42, recodeBinary(42))
        assertEquals(42L, recodeBinary(42L))
        assertEquals(3.14, recodeBinary(3.14))
        assertEquals(true, recodeBinary(true))
        assertEquals(false, recodeBinary(false))
        assertEquals(instant, recodeBinary(instant))
        assertEquals(null, recodeBinary<String?>(null))
        assertEquals(testObj, recodeBinary(testObj))
    }

    @Test
    fun `Verify no issues with extra fields`() {
        val input = buildDocument {
            "str" put "Test"
            "num" put 123
            "flag" put true
            "extraField1" put "Extra1"
            "extraField2" put 999
        }
        val decoded = Kodein().decode<TestClass>(input)
        assertEquals("Test", decoded.str)
        assertEquals(123, decoded.num)
        assertEquals(true, decoded.flag)
    }

    @Test
    fun `Verify BsonDocument within class`() {
        val original = buildDocument {
            "id" put "doc1"
            "label" put "Test Document"
            "content" put {
                "field1" put "value1"
                "field2" put 42
                "nested" put buildDocument {
                    "innerField" put true
                }
            }
        }

        val introspect = Kodein().introspect(original)
        val decoded = introspect.asClass<ClassWithDocument>()
        assertEquals("doc1", decoded.id)
        assertEquals("Test Document", decoded.label)
        assertEquals(original.getDocument("content"), decoded.content)

        val bson = Kodein().encode(decoded)
        assertEquals(original, bson)
    }

    @Test
    fun `Test numeric type coercion`() {
        val input = buildDocument {
            "intAsLong" put 123L
            "longAsInt" put 456
            "doubleAsInt" put 78
            "intAsDouble" put 9.0
            "date" put Instant.fromEpochMilliseconds(100L).truncatedToMillis().toString()
        }
        val json = input.toJson(
            JsonWriterSettings.builder().outputMode(JsonMode.RELAXED)
                .dateTimeConverter { lng, writer -> writer.writeString(lng.toString()) }
                .int64Converter { lng, writer -> writer.writeNumber(lng.toString()) }
                .build())
        val parsedJson = BsonDocument.parse(json)
        val decoded = Kodein().decode<NumericTypes>(parsedJson)
        assertEquals(123, decoded.intAsLong)
        assertEquals(456L, decoded.longAsInt)
        assertEquals(78.0, decoded.doubleAsInt)
        assertEquals(9, decoded.intAsDouble)
        assertEquals(Instant.fromEpochMilliseconds(100L), decoded.date)
    }


    @Serializable
    data class TestClass(
        val str: String,
        val num: Int,
        val flag: Boolean,
    )

    @Serializable
    data class ClassWithDocument(
        val id: String,
        val label: String,
        val content: @Contextual BsonDocument
    )

    @Serializable
    data class NumericTypes(
        val intAsLong: Int,
        val longAsInt: Long,
        val doubleAsInt: Double,
        val intAsDouble: Int,
        val date: KInstant
    )
}