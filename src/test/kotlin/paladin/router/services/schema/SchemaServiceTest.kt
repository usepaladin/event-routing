package paladin.router.services.schema

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.github.oshai.kotlinlogging.KLogger
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import org.apache.avro.generic.GenericRecord
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import paladin.avro.ChangeEventData
import paladin.avro.ChangeEventOperation
import util.TestUtilServices
import util.mock.User
import java.util.*


@ExtendWith(MockKExtension::class)
class SchemaServiceTest {

    @MockK
    private lateinit var logger: KLogger

    private lateinit var schemaService: SchemaService
    private val objectMapper = ObjectMapper()

    @BeforeEach
    fun setUp() {
        schemaService = SchemaService(TestUtilServices.objectMapper, logger)
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
    fun `handle avro encoding from another avro object`() {
        val schema = """
              {
                    "type": "record",
                    "name": "ChangeEventData",
                    "fields": [
                      {
                        "name": "operation",
                        "type": {
                          "type": "enum",
                          "name": "ChangeEventOperation",
                          "symbols": [
                            "CREATE",
                            "UPDATE",
                            "DELETE",
                            "SNAPSHOT",
                            "OTHER"
                          ]
                        }
                      },
                      {
                        "name": "before",
                        "type": ["null",{
                          "type": "map",
                          "values": "string"
                        }],
                        "default": null
                      },
                      {
                        "name": "after",
                        "type": ["null",{
                          "type": "map",
                          "values": "string"
                        }],
                        "default": null
                      },
                      {
                        "name": "source",
                        "type": ["null",{
                          "type": "map",
                          "values": "string"
                        }],
                        "default": null
                      },
                      {
                        "name": "timestamp",
                        "type": ["null", "long"],
                        "default": null
                      },
                      {
                        "name": "table",
                        "type": ["null", "string"],
                        "default": null
                      }
                    ]
                  }
        """.trimIndent()
        val event = ChangeEventData(
            ChangeEventOperation.CREATE,
            mapOf(),
            mapOf(),
            mapOf(),
            Date().time,
            "test"
        )

        val record: GenericRecord = schemaService.parseToAvro(schema, event)
        assertEquals(ChangeEventOperation.CREATE.toString(), record["operation"].toString())
        assertEquals(0, (record["before"] as Map<*, *>).size)
        assertEquals(0, (record["after"] as Map<*, *>).size)
        assertEquals(0, (record["source"] as Map<*, *>).size)
        assertEquals("test", record["table"].toString())
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


