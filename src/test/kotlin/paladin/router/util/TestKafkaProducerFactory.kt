package paladin.router.util

import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.serializer.JsonSerializer
import org.testcontainers.kafka.ConfluentKafkaContainer
import paladin.router.enums.configuration.Broker

object TestKafkaProducerFactory {


    fun createKafkaTemplate(
        container: ConfluentKafkaContainer,
        keySerializer: Broker.BrokerFormat,
        valueSerializer: Broker.BrokerFormat
    ): KafkaTemplate<Any, Any> {
        val keySerializerClass = fromFormat(keySerializer)
        val valueSerializerClass = fromFormat(valueSerializer)

        val producerProps = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to container.bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to keySerializerClass,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to valueSerializerClass,
            ProducerConfig.ACKS_CONFIG to "all",
            ProducerConfig.RETRIES_CONFIG to 3,
            ProducerConfig.LINGER_MS_CONFIG to 1,
            ProducerConfig.BATCH_SIZE_CONFIG to 16384


        )
        val producerFactory = DefaultKafkaProducerFactory<Any, Any>(producerProps)
        return KafkaTemplate(producerFactory)
    }

    private fun fromFormat(format: Broker.BrokerFormat): String {
        return when (format) {
            Broker.BrokerFormat.STRING -> StringSerializer::class.java.name
            Broker.BrokerFormat.JSON -> JsonSerializer::class.java.name
            Broker.BrokerFormat.AVRO -> KafkaAvroSerializer::class.java.name
        }
    }

}