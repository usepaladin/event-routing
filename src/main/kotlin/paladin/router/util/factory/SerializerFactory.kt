package paladin.router.util.factory

import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.confluent.kafka.serializers.KafkaJsonSerializer
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer
import org.apache.kafka.common.serialization.StringSerializer
import paladin.router.enums.configuration.Broker


object SerializerFactory {
    fun fromFormat(format: Broker.ProducerFormat?, enforceSchema: Boolean = false): String {
        return when (format) {
            Broker.ProducerFormat.STRING, null -> StringSerializer::class.java.name
            Broker.ProducerFormat.JSON -> {
                if (enforceSchema) {
                    KafkaJsonSchemaSerializer::class.java.name
                } else {
                    KafkaJsonSerializer::class.java.name
                }

            }

            Broker.ProducerFormat.AVRO -> KafkaAvroSerializer::class.java.name
        }
    }
}
