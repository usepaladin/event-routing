package paladin.router.services.schema

import io.github.oshai.kotlinlogging.KLogger
import io.mockk.impl.annotations.MockK
import org.springframework.boot.test.context.SpringBootTest

import io.mockk.verify
import org.apache.avro.generic.GenericRecord
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows


@SpringBootTest
class SchemaServiceTest {

        @MockK
        private lateinit var logger: KLogger

        private lateinit var schemaService: SchemaService

        private data class User(val name: String, val age: Int)

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

        @Test
        fun `encodeToAvro should convert Kotlin object to GenericRecord`() {
            val user = User(name = "Alice", age = 30)

            val record: GenericRecord = schemaService.encodeToAvro(avroSchemaString, user)

            assertEquals("Alice", record["name"].toString())
            assertEquals(30, record["age"])
        }

        @Test
        fun `encodeToAvro should log and throw exception on invalid schema`() {
            val invalidSchema = """{ "type": "record", "name": "Bad" }""" // no fields, will throw

            val user = User("Bob", 25)

            assertThrows<Exception> {
                schemaService.encodeToAvro(invalidSchema, user)
            }
        }
    }


