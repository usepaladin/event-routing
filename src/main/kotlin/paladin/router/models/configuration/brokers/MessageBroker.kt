package paladin.router.models.configuration.brokers

import paladin.router.entities.brokers.configuration.MessageBrokerConfigurationEntity

import paladin.router.enums.configuration.Broker
import paladin.router.util.Configurable
import java.time.ZonedDateTime
import java.util.*

data class MessageBroker(
    val id: UUID,
    val brokerName: String,
    val brokerType: Broker.BrokerType,
    val valueSerializationFormat: Broker.BrokerFormat,
    val keySerializationFormat: Broker.BrokerFormat?,
    var defaultBroker: Boolean,
    val createdAt: ZonedDateTime,
    var updatedAt: ZonedDateTime
): Configurable {
    override fun updateConfiguration(config: Configurable): Configurable {
        if (config is MessageBroker) {
            this.defaultBroker = config.defaultBroker
            this.updatedAt = ZonedDateTime.now()
        }
        return this
    }

    companion object Factory {
        fun fromEntity(entity: MessageBrokerConfigurationEntity): MessageBroker {
            return MessageBroker(
                id = entity.id ?: throw IllegalArgumentException("BrokerTopic ID cannot be null"),
                brokerName = entity.brokerName,
                brokerType = entity.brokerType,
                keySerializationFormat = entity.keyFormat,
                valueSerializationFormat = entity.valueFormat,
                defaultBroker = entity.defaultBroker,
                createdAt = entity.createdAt,
                updatedAt = entity.updatedAt
            )
        }
    }
}
