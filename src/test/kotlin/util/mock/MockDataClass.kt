package util.mock

import paladin.avro.ChangeEventData
import paladin.avro.ChangeEventOperation
import paladin.avro.EventType
import paladin.avro.MockKeyAv
import java.util.*

data class Operation(val id: String, val operation: OperationType) {
    enum class OperationType {
        CREATE,
        UPDATE,
        DELETE
    }

    companion object {
        val SCHEMA = """
            {
                "${'$'}schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "id": {
                        "type": "string"
                    },
                    "operation": {
                        "type": "string",
                        "enum": ["CREATE", "UPDATE", "DELETE"]
                    }
                },
                "required": ["id", "operation"]
            }
        """.trimIndent()
    }
}

data class User(val name: String, val age: Int, val email: String? = null) {
    companion object {
        val SCHEMA = """
            {
                "${'$'}schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": false,
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string"
                    },
                    "age": {
                        "type": "integer"
                    },
                    "email": {
                        "type": "string",
                        "format": "email"
                    }
                },
                "required": ["name", "age"]
            }
        """.trimIndent()
    }
}


fun mockAvroKey() = MockKeyAv(
    UUID.randomUUID().toString(),
    EventType.CREATE
)

fun mockAvroPayload() = ChangeEventData(
    ChangeEventOperation.CREATE,
    null,
    mapOf(
        "id" to "123",
        "name" to "Test Name",
        "description" to "Test Description"
    ),
    mapOf(
        "id" to "123",
        "name" to "Test Name",
        "description" to "Test Description"
    ),
    Date().toInstant().epochSecond,
    "user"
)

/**
 * Modified version of the ChangeEventData Avro schema to include a subset of the original fields
 *
 * Includes fields: operation, before, and after
 * Negates fields: table, source, timestamp
 */
fun mockModifiedAvroSchema() = """{
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
          }
        ]
      }""".trimIndent()