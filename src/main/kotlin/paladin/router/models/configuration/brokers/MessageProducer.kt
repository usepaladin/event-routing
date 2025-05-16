package paladin.router.models.configuration.brokers

import paladin.router.entities.brokers.configuration.MessageBrokerConfigurationEntity

import paladin.router.enums.configuration.Broker
import paladin.router.util.Configurable
import java.time.ZonedDateTime
import java.util.*

data class MessageProducer(
    val id: UUID,
    val brokerName: String,
    val brokerType: Broker.BrokerType,
    val valueSerializationFormat: Broker.ProducerFormat,
    val keySerializationFormat: Broker.ProducerFormat?,
    var defaultBroker: Boolean,
    val createdAt: ZonedDateTime,
    var updatedAt: ZonedDateTime
) : Configurable {
    override fun updateConfiguration(config: Configurable): Configurable {
        if (config is MessageProducer) {
            this.defaultBroker = config.defaultBroker
            this.updatedAt = ZonedDateTime.now()
        }
        return this
    }

    companion object Factory {
        fun fromEntity(entity: MessageBrokerConfigurationEntity): MessageProducer {
            return MessageProducer(
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
