package paladin.router.util

import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.serializer.JsonSerializer
import org.testcontainers.kafka.ConfluentKafkaContainer
import paladin.avro.database.ChangeEventData
import paladin.avro.database.ChangeEventOperation
import paladin.router.enums.configuration.Broker
import java.util.*

object TestKafkaProducerFactory {
    fun createKafkaTemplate(
        container: ConfluentKafkaContainer,
        key: Broker.BrokerFormat,
        value: Broker.BrokerFormat,
        schemaRegistryUrl: String? = null
    ): KafkaTemplate<Any, Any> {
        val keySerializerClass = fromFormat(key)
        val valueSerializerClass = fromFormat(value)

        if (includesAvro(key, value) && schemaRegistryUrl != null) {
            throw IllegalArgumentException("Schema Registry URL must be provided when testing Avro serialization.")
        }

        val producerProps = mutableMapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to container.bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to keySerializerClass,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to valueSerializerClass,
            ProducerConfig.ACKS_CONFIG to "all",
            ProducerConfig.RETRIES_CONFIG to 3,
            ProducerConfig.LINGER_MS_CONFIG to 1,
            ProducerConfig.BATCH_SIZE_CONFIG to 16384
        )
        if (includesAvro(key, value)) {
            producerProps["schema.registry.url"] = schemaRegistryUrl
            producerProps[KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG] = schemaRegistryUrl
        }
        val producerFactory = DefaultKafkaProducerFactory<Any, Any>(producerProps.toMap())
        return KafkaTemplate(producerFactory)
    }

    private fun fromFormat(format: Broker.BrokerFormat): String {
        return when (format) {
            Broker.BrokerFormat.STRING -> StringSerializer::class.java.name
            Broker.BrokerFormat.JSON -> JsonSerializer::class.java.name
            Broker.BrokerFormat.AVRO -> KafkaAvroSerializer::class.java.name
        }
    }

    private fun includesAvro(key: Broker.BrokerFormat, value: Broker.BrokerFormat): Boolean {
        return key == Broker.BrokerFormat.AVRO || value == Broker.BrokerFormat.AVRO
    }

    fun mockAvroPayload() = ChangeEventData(
        ChangeEventOperation.CREATE,
        null,
        mapOf(
            "id" to "123",
            "name" to "Test Name",
            "description" to "Test Description"
        ),
        mapOf(
            "id" to "123",
            "name" to "Test Name",
            "description" to "Test Description"
        ),
        Date().toInstant().epochSecond,
        "user"
    )

}