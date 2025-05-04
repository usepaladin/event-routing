package paladin.router.util

import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient

object SchemaRegistryFactory {

    fun init(schemaRegistryUrl: String, schemas: List<SchemaRegistrationOperation>): CachedSchemaRegistryClient {
        return CachedSchemaRegistryClient(schemaRegistryUrl, 100).also { client ->
            schemas.forEach {
                val (schema, topic, type) = it
                registerSchema(client, schema, topic, type)
            }
        }

    }

    fun registerSchema(
        client: CachedSchemaRegistryClient,
        schema: AvroSchema,
        topic: String,
        type: SchemaRegistrationOperation.SchemaType
    ): Unit {
        val subject = when (type) {
            SchemaRegistrationOperation.SchemaType.VALUE -> "${topic}-value"
            SchemaRegistrationOperation.SchemaType.KEY -> "${topic}-key"
        }
        client.register(subject, schema)
    }
}

data class SchemaRegistrationOperation(
    val schema: AvroSchema,
    val topic: String,
    val type: SchemaType
) {

    enum class SchemaType {
        VALUE,
        KEY
    }
}
