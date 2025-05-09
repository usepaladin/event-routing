package paladin.router.util.factory

import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.confluent.kafka.serializers.KafkaJsonSerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.kafka.support.serializer.JsonSerializer
import paladin.router.enums.configuration.Broker


object SerializerFactory {
    fun fromFormat(format: Broker.BrokerFormat?, enforceSchema: Boolean = false): String {
        return when (format) {
            Broker.BrokerFormat.STRING, null -> StringSerializer::class.java.name
            Broker.BrokerFormat.JSON -> {
                if (enforceSchema) {
                    KafkaJsonSerializer::class.java.name
                } else {
                    JsonSerializer::class.java.name
                }

            }

            Broker.BrokerFormat.AVRO -> KafkaAvroSerializer::class.java.name
        }
    }
}
