package paladin.router.entities.brokers.configuration

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.convertValue
import io.hypersistence.utils.hibernate.type.json.JsonBinaryType
import jakarta.persistence.*
import org.hibernate.annotations.Type
import paladin.router.enums.configuration.Broker.BrokerType
import paladin.router.enums.configuration.Broker.BrokerFormat
import paladin.router.models.configuration.brokers.MessageBroker
import paladin.router.pojo.configuration.brokers.auth.EncryptedBrokerConfig
import paladin.router.pojo.configuration.brokers.core.BrokerConfig
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
    name = "message_brokers",
    schema = "event_routing",
    uniqueConstraints = [
        UniqueConstraint(columnNames = ["broker_name"])
    ],
    indexes = [
        Index(name = "idx_message_brokers_binder_name", columnList = "broker_name"),


    ]
)
data class MessageBrokerConfigurationEntity(
    @Id
    @GeneratedValue
    @Column(name = "id", columnDefinition = "UUID DEFAULT uuid_generate_v4()", nullable = false)
    val id: UUID? = null,

    @Column(name = "broker_name", nullable = false, unique = true)
    var brokerName: String,

    @Column(name = "broker_type", nullable = false)
    @Enumerated(EnumType.STRING)
    val brokerType: BrokerType,

    @Column(name = "key_format", nullable = true)
    @Enumerated(EnumType.STRING)
    val keyFormat: BrokerFormat?,

    @Column(name = "value_format", nullable = false)
    @Enumerated(EnumType.STRING)
    val valueFormat: BrokerFormat,

    @JsonIgnore
    @Column(name = "enc_broker_config", nullable = false)
    var brokerConfigEncrypted: String,

    @Type(JsonBinaryType::class)
    @Column(name = "broker_config", nullable = false, columnDefinition = "JSONB")
    var brokerConfig: Map<String, Any>,

    @Column(name = "default_broker")
    var defaultBroker: Boolean = false,

    @Column(name = "created_at", columnDefinition = "TIMESTAMP DEFAULT CURRENT_TIMESTAMP", updatable = false)
    var createdAt: ZonedDateTime = ZonedDateTime.now(),

    @Column(name = "updated_at", columnDefinition = "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
    var updatedAt: ZonedDateTime = ZonedDateTime.now()
){
    constructor(
        brokerName: String,
        brokerType: BrokerType,
        keyFormat: BrokerFormat?,
        valueFormat: BrokerFormat,
        brokerConfigEncrypted: String,
        brokerConfig: Map<String, Any>,
        defaultBroker: Boolean = false
    ) : this(
        null,
        brokerName,
        brokerType,
        keyFormat,
        valueFormat,
        brokerConfigEncrypted,
        brokerConfig,
        defaultBroker
    )

    companion object Factory{
        fun fromConfiguration(messageBroker: MessageBroker, encryptedConfig: String, brokerConfig: BrokerConfig): MessageBrokerConfigurationEntity {
            val objectMapper = ObjectMapper()

            return MessageBrokerConfigurationEntity(
                id = messageBroker.id,
                brokerName = messageBroker.brokerName,
                brokerType = messageBroker.brokerType,
                keyFormat = messageBroker.keySerializationFormat,
                valueFormat = messageBroker.valueSerializationFormat,
                brokerConfigEncrypted = encryptedConfig,
                brokerConfig = objectMapper.convertValue(brokerConfig),
                defaultBroker = messageBroker.defaultBroker,
                createdAt = messageBroker.createdAt,
                updatedAt = messageBroker.updatedAt
            )
        }
    }
}
