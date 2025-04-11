package paladin.router.services.schema

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.github.oshai.kotlinlogging.KLogger
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import org.springframework.boot.test.context.SpringBootTest

import io.mockk.verify
import org.apache.avro.generic.GenericRecord
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith


@ExtendWith(MockKExtension::class)
class SchemaServiceTest {

        @MockK
        private lateinit var logger: KLogger

        private lateinit var schemaService: SchemaService

        private data class User(val name: String, val age: Int, val email: String? = null)
        private val objectMapper = ObjectMapper()

        @BeforeEach
        fun setUp() {
            schemaService = SchemaService(logger)
        }

        private val avroSchemaString =
            """
        {
          "type": "record",
          "name": "User",
          "namespace": "com.example",
          "fields": [
            { "name": "name", "type": "string" },
            { "name": "age", "type": "int" }
          ]
        }
            """.trimIndent()

    private val jsonSchema = """
        {
          "type": "object",
          "properties": {
            "name": { "type": "string" },
            "age": { "type": "number" }
          },
          "required": ["name", "age"]
        }
    """.trimIndent()

        @Test
        fun `encodeToAvro should convert Kotlin object to GenericRecord`() {
            val user = User(name = "Alice", age = 30)

            val record: GenericRecord = schemaService.parseToAvro(avroSchemaString, user)
            assertEquals("Alice", record["name"].toString())
            assertEquals(30, record["age"])
        }

        @Test
        fun `encodeToAvro should log and throw exception on invalid schema`() {
            val invalidSchema = """{ "type": "record", "name": "Bad" }""" // no fields, will throw

            val user = User("Bob", 25)

            assertThrows<Exception> {
                schemaService.parseToAvro(invalidSchema, user)
            }
        }

    @Test
    fun `parseToJson should filter only fields present in the schema`() {
        val user = User("Bob", 42, "bob@example.com")

        val result: JsonNode = schemaService.parseToJson(jsonSchema, user)

        assertTrue(result.has("name"))
        assertTrue(result.has("age"))
        assertFalse(result.has("email"))

        assertEquals("Bob", result["name"].asText())
        assertEquals(42, result["age"].asInt())
    }

    @Test
    fun `parseToJson should return empty object for unmatched fields`() {
        val jsonSchema = """{"type":"object","properties":{"foo":{"type":"string"}}}"""
        val user = User("Zoe", 25, "zoe@example.com")

        val result = schemaService.parseToJson(jsonSchema, user)
        assertEquals(0, result.size())
    }

    @Test
    fun `parseToString should serialize object to JSON string`() {
        val user = User("Charlie", 50, "charlie@example.com")
        val json = schemaService.parseToString(user)

        val parsed = objectMapper.readTree(json)
        assertEquals("Charlie", parsed["name"].asText())
        assertEquals(50, parsed["age"].asInt())
        assertEquals("charlie@example.com", parsed["email"].asText())
    }
    }


