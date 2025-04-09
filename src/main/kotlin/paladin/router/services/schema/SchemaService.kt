package paladin.router.services.schema

import com.fasterxml.jackson.databind.ObjectMapper
import io.github.oshai.kotlinlogging.KLogger
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.Decoder
import org.apache.avro.io.DecoderFactory
import org.springframework.stereotype.Service
import java.io.IOException
import kotlin.jvm.Throws

@Service
class SchemaService(private val logger: KLogger) {

    private final val objectMapper = ObjectMapper()

    @Throws(IOException::class)
    fun <T> encodeToAvro  (schema: String, payload: T ): GenericRecord{
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
}