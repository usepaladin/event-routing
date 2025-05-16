package paladin.router.util.factory

import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.confluent.kafka.serializers.KafkaJsonSerializer
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer
import org.apache.kafka.common.serialization.StringSerializer
import paladin.router.enums.configuration.Broker


object SerializerFactory {
    fun fromFormat(format: Broker.ProducerFormat?, enforceSchema: Boolean = false): String {
        return when (format) {
            Broker.ProducerFormat.STRING, null -> "org.apache.kafka.common.serialization.StringSerializer"
            Broker.ProducerFormat.JSON -> {
                if (enforceSchema) {
                    "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer"
                } else {
                    "org.apache.kafka.common.serialization.StringSerializer" // Fallback to string for JSON without schema
                }
            }
            Broker.ProducerFormat.AVRO -> {
                if (!enforceSchema) {
                    throw IllegalArgumentException("AVRO serialization requires a schema registry")
                }
                "io.confluent.kafka.serializers.KafkaAvroSerializer"
            }
        }
    }
    }
}
