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
import java.io.IOException

@Service
class SchemaService(
    private val objectMapper: ObjectMapper,
    private val logger: KLogger
) {


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

    fun <T> parseToString(payload: T): String {
        // When handling Avro messages, we would need to ensure we only convert the payload to a string, not any associated metadata included in the object (ie. Schema)
        if (payload is SpecificRecord) {
            val avroPayload = destructureAvro(payload as GenericRecord)
            return objectMapper.writeValueAsString(avroPayload)
        }

        return objectMapper.writeValueAsString(payload)
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