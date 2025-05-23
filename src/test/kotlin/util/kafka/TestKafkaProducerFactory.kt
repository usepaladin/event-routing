package util.kafka

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.testcontainers.kafka.ConfluentKafkaContainer
import paladin.router.enums.configuration.Broker
import paladin.router.util.factory.SerializerFactory

object TestKafkaProducerFactory {
    inline fun <reified K : Any, reified V : Any> createKafkaTemplate(
        container: ConfluentKafkaContainer,
        keyFormat: Broker.ProducerFormat,
        valueFormat: Broker.ProducerFormat,
        schemaRegistryUrl: String? = null
    ): KafkaTemplate<K, V> {
        val keySerializerClass = SerializerFactory.fromFormat(keyFormat, !schemaRegistryUrl.isNullOrEmpty())
        val valueSerializerClass = SerializerFactory.fromFormat(valueFormat, !schemaRegistryUrl.isNullOrEmpty())

        val producerProps = mutableMapOf<String, Any>(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to container.bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to keySerializerClass,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to valueSerializerClass,
            ProducerConfig.ACKS_CONFIG to "all",
            ProducerConfig.RETRIES_CONFIG to 3,
            ProducerConfig.LINGER_MS_CONFIG to 1,
            ProducerConfig.BATCH_SIZE_CONFIG to 16384
        )

        schemaRegistryUrl?.let {
            producerProps[AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG] = it
            producerProps[AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS] = false
        }

        val producerFactory = DefaultKafkaProducerFactory<K, V>(producerProps)
        return KafkaTemplate(producerFactory)
    }

}