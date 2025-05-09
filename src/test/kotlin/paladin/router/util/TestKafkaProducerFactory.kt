package paladin.router.util

import com.fasterxml.jackson.databind.JsonNode
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.testcontainers.kafka.ConfluentKafkaContainer
import paladin.avro.ChangeEventData
import paladin.avro.ChangeEventOperation
import paladin.avro.EventType
import paladin.avro.MockKeyAv
import paladin.router.enums.configuration.Broker
import paladin.router.util.TestUtilServices.objectMapper
import paladin.router.util.factory.SerializerFactory
import java.util.*

object TestKafkaProducerFactory {
    fun createKafkaTemplate(
        container: ConfluentKafkaContainer,
        key: Broker.BrokerFormat,
        value: Broker.BrokerFormat,
        schemaRegistryUrl: String? = null
    ): KafkaTemplate<Any, Any> {
        val keySerializerClass = SerializerFactory.fromFormat(key, !schemaRegistryUrl.isNullOrEmpty())
        val valueSerializerClass = SerializerFactory.fromFormat(value, !schemaRegistryUrl.isNullOrEmpty())

        val producerProps = mutableMapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to container.bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to keySerializerClass,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to valueSerializerClass,
            ProducerConfig.ACKS_CONFIG to "all",
            ProducerConfig.RETRIES_CONFIG to 3,
            ProducerConfig.LINGER_MS_CONFIG to 1,
            ProducerConfig.BATCH_SIZE_CONFIG to 16384
        )

        schemaRegistryUrl?.let {
            producerProps["schema.registry.url"] = it
            producerProps[KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG] = it
        }

        val producerFactory = DefaultKafkaProducerFactory<Any, Any>(producerProps.toMap())
        return KafkaTemplate(producerFactory)
    }

    fun mockAvroKey() = MockKeyAv(
        UUID.randomUUID().toString(),
        EventType.CREATE
    )

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

    fun mockJsonKey(): JsonNode =
        objectMapper.readTree(
            """
            {
                "id": "${UUID.randomUUID()}",
                "eventType": "${EventType.CREATE}"
            }
        """.trimIndent()
        )

    fun mockJsonPayload(): JsonNode =
        objectMapper.readTree(
            """
            {
                "id": "123",
                "name": "Test Name",
                "description": "Test Description"
            }
        """.trimIndent()
        )

}