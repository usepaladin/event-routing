package paladin.router.entities.brokers.configuration

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.convertValue
import io.hypersistence.utils.hibernate.type.json.JsonBinaryType
import jakarta.persistence.*
import org.hibernate.annotations.Type
import paladin.router.enums.configuration.Broker.BrokerType
import paladin.router.enums.configuration.Broker.ProducerFormat
import paladin.router.models.configuration.brokers.MessageProducer
import paladin.router.models.configuration.brokers.core.ProducerConfig
import java.time.ZonedDateTime
import java.util.*

/**
 * Represents the configuration details about each user provided message broker that will receive messages from the database change events
 *
 * All sensitive configuration details associated with the connection of a message broker, will be default,
 * be encrypted when stored in the database, and decrypted upon retrieval, this would include:
 *  - Connection details (IP, Port, etc)
 *  - Authentication details (if required)
 *
 * Less sensitive details would be stored in a Binary JSON format, this would include:
 * - Binder Configuration details
 * - Topic Configuration details
 * - Producer Configuration details
 * - Consumer Configuration details
 *
 * If the server instance does not require encryption, the object will be stored as a JSON object in string form
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@Entity
@Table(
    name = "message_producers",
    schema = "event_routing",
    uniqueConstraints = [
        UniqueConstraint(columnNames = ["producer_name"])
    ],
    indexes = [
        Index(name = "idx_message_producers_producer_name", columnList = "producer_name"),
    ]
)
data class MessageBrokerConfigurationEntity(
    @Id
    @GeneratedValue
    @Column(name = "id", columnDefinition = "UUID DEFAULT uuid_generate_v4()", nullable = false)
    val id: UUID? = null,

    @Column(name = "producer_name", nullable = false, unique = true)
    var producerName: String,

    @Column(name = "broker_type", nullable = false)
    @Enumerated(EnumType.STRING)
    val brokerType: BrokerType,

    @Column(name = "key_format", nullable = true)
    @Enumerated(EnumType.STRING)
    val keyFormat: ProducerFormat?,

    @Column(name = "value_format", nullable = false)
    @Enumerated(EnumType.STRING)
    val valueFormat: ProducerFormat,

    @JsonIgnore
    @Column(name = "enc_producer_config", nullable = false)
    var producerConfigEncrypted: String,

    @Type(JsonBinaryType::class)
    @Column(name = "producer_config", nullable = false, columnDefinition = "JSONB")
    var brokerConfig: Map<String, Any>,

    @Column(name = "default_producer")
    var defaultProducer: Boolean = false,

    @Column(name = "created_at", columnDefinition = "TIMESTAMP DEFAULT CURRENT_TIMESTAMP", updatable = false)
    var createdAt: ZonedDateTime = ZonedDateTime.now(),

    @Column(name = "updated_at", columnDefinition = "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
    var updatedAt: ZonedDateTime = ZonedDateTime.now()
) {
    companion object Factory {
        fun fromConfiguration(
            messageProducer: MessageProducer,
            encryptedConfig: String,
            producerConfig: ProducerConfig
        ): MessageBrokerConfigurationEntity {
            val objectMapper = ObjectMapper()
            messageProducer.let {
                return MessageBrokerConfigurationEntity(
                    id = it.id,
                    producerName = it.producerName,
                    brokerType = it.brokerType,
                    keyFormat = it.keySerializationFormat,
                    valueFormat = it.valueSerializationFormat,
                    producerConfigEncrypted = encryptedConfig,
                    brokerConfig = objectMapper.convertValue(producerConfig),
                    defaultProducer = it.defaultProducer,
                    createdAt = it.createdAt,
                    updatedAt = it.updatedAt
                )
            }
        }
    }
}
