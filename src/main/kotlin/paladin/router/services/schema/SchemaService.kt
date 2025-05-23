package paladin.router.services.schema

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import io.github.oshai.kotlinlogging.KLogger
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecord
import org.springframework.stereotype.Service
import paladin.router.enums.configuration.Broker
import java.io.IOException

@Service
class SchemaService(
    private val objectMapper: ObjectMapper,
    private val logger: KLogger
) {

    /**
     * Converts a payload to the appropriate format based on the topic's serialisation technique
     * Also utilises any provided schema to parse the message (When using Json or Avro)
     *
     * @param payload The value being transformed
     * @param format The format of the payload
     * @param schema Any associated schema to validate and transform the payload into a specific format
     *
     * @return A transformed value
     */
    fun <T> convertToFormat(payload: T, format: Broker.ProducerFormat, schema: String?): Any {
        return when (format) {
            Broker.ProducerFormat.STRING -> parseToString(payload)
            Broker.ProducerFormat.JSON -> {
                if (schema == null) {
                    return parseToJson(payload)

                }
                return parseToJson(schema, payload)
            }

            Broker.ProducerFormat.AVRO -> {
                if (schema == null) {
                    throw IllegalArgumentException("Schema cannot be null for Avro format")
                }
                return parseToAvro(schema, payload)
            }
        }
    }


    /**
     * Parses the Payload into an Avro GenericRecord using the provided Avro schema.
     */
    @Throws(IOException::class)
    fun <T> parseToAvro(schema: String, payload: T): GenericRecord {
        val payloadMap: Map<String, Any?> = destructureClass(payload)
        try {
            // Parse the schema string into an Avro schema
            val avroSchema: Schema = Schema.Parser().parse(schema)
            val record = GenericData.Record(avroSchema)
            avroSchema.fields.forEach { field ->
                val value = payloadMap[field.name()]
                record.put(field.name(), wrapAvroValue(field.schema(), value))
            }

            return record
        } catch (e: Exception) {
            logger.error(e) { "Error encoding payload to Avro: ${e.message}" }
            throw e
        }
    }

    private fun wrapAvroValue(schema: Schema, value: Any?): Any? {
        return when (schema.type) {
            Schema.Type.UNION -> {
                val nonNullSchema = schema.types.find { it.type != Schema.Type.NULL }
                if (value == null) null
                else wrapAvroValue(nonNullSchema!!, value)
            }

            Schema.Type.MAP -> {
                @Suppress("UNCHECKED_CAST")
                val mapValue = value as? Map<String, Any?>
                mapValue?.mapValues { (_, v) -> v?.toString() } // Convert values to string if required
            }

            Schema.Type.ENUM -> {
                GenericData.EnumSymbol(schema, value.toString())
            }

            Schema.Type.LONG -> {
                when (value) {
                    is Number -> value.toLong()
                    is String -> value.toLongOrNull()
                    else -> null
                }
            }

            Schema.Type.STRING -> value?.toString()
            else -> value
        }
    }

    /**
     * Parses a JSON string into a JsonNode object in the form of the provided JSON Schema.
     * Object will be validated against the schema to ensure it is valid and contains all required attributes,
     * and will parse into an object only containing the attributes defined in the schema.
     */
    fun <T> parseToJson(schema: String, payload: T): JsonNode {
        try {
            val schemaNode = objectMapper.readTree(schema)
            val allowedFields = schemaNode["properties"]?.fieldNames()?.asSequence()?.toSet().orEmpty()

            val payloadNode = objectMapper.valueToTree<ObjectNode>(payload)
            val filteredNode = objectMapper.createObjectNode()

            allowedFields.forEach { field ->
                if (payloadNode.has(field)) {
                    filteredNode.set<JsonNode>(field, payloadNode.get(field))
                } else {
                    val required = schemaNode["properties"]?.get(field)?.get("required")?.asBoolean() ?: false
                    if (required) {
                        throw IllegalArgumentException("Missing required field: $field")
                    }
                }
            }

            return filteredNode
        } catch (e: Exception) {
            logger.error(e) { "Error creating Json Object with provided schema: ${e.message}" }
            throw e
        }
    }

    /**
     * Parses a JSON string into a JsonNode object
     */
    fun <T> parseToJson(payload: T): JsonNode {
        try {
            val payloadNode = objectMapper.valueToTree<ObjectNode>(payload)
            return payloadNode
        } catch (e: Exception) {
            logger.error(e) { "Error creating JSON Node: ${e.message}" }
            throw e
        }
    }

    /**
     * Converts the payload to a string without JSON serialization for simple types.
     * For complex types or Avro records, serializes to a string representation of the data.
     */
    fun <T> parseToString(payload: T): String {
        return when (payload) {
            is String -> payload // Return raw string without JSON serialization
            is SpecificRecord -> {
                val avroPayload = destructureAvro(payload as GenericRecord)
                objectMapper.writeValueAsString(avroPayload)
            }

            else -> {
                // For other types, convert to string representation without quotes if possible
                payload?.toString() ?: ""
            }
        }
    }


    private fun <T> destructureClass(payload: T): Map<String, Any?> {
        if (payload is SpecificRecord) {
            return destructureAvro(payload as GenericRecord)
        }

        return objectMapper.convertValue(payload, object : TypeReference<Map<String, Any?>>() {})
    }

    private fun <T : GenericRecord> destructureAvro(payload: T): Map<String, Any?> {
        val schema = payload.schema
        val fields = schema.fields
        val map = mutableMapOf<String, Any?>()
        for (field in fields) {
            val fieldName = field.name()
            val fieldValue = payload.get(fieldName)
            map[fieldName] = fieldValue
        }
        return map
    }
}