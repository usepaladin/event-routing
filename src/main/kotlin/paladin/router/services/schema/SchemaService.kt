package paladin.router.services.schema

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import io.github.oshai.kotlinlogging.KLogger
import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.Decoder
import org.apache.avro.io.DecoderFactory
import org.springframework.stereotype.Service
import java.io.IOException
import kotlin.jvm.Throws

@Service
class SchemaService(private val logger: KLogger) {

    private final val objectMapper = ObjectMapper()

    /**
     * Parses the Payload into an Avro GenericRecord using the provided Avro schema.
     */
    @Throws(IOException::class)
    fun <T> parseToAvro  (schema: String, payload: T ): GenericRecord{
        try{
        // Parse the schema string into an Avro schema
        val avroSchema: Schema = Schema.Parser().parse(schema)

        // Encode payload into a Json Object for conversion
        val jsonObject = objectMapper.writeValueAsString(payload)

        // Create a GenericRecord using the Avro schema
        val decoder: Decoder = DecoderFactory.get().jsonDecoder(avroSchema, jsonObject)
        val reader = GenericDatumReader<GenericRecord>(avroSchema)
        return reader.read(null, decoder)
        } catch (e: Exception) {
            logger.error(e) { "Error encoding payload to Avro: ${e.message}" }
            throw e
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

            allowedFields.forEach{field ->
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
        return objectMapper.writeValueAsString(payload)
    }
}