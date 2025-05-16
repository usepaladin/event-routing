package paladin.router.models.configuration.brokers

import paladin.router.entities.brokers.configuration.MessageBrokerConfigurationEntity

import paladin.router.enums.configuration.Broker
import paladin.router.util.Configurable
import java.time.ZonedDateTime
import java.util.*

data class MessageProducer(
    val id: UUID,
    val producerName: String,
    val brokerType: Broker.BrokerType,
    val valueSerializationFormat: Broker.ProducerFormat,
    val keySerializationFormat: Broker.ProducerFormat?,
    var defaultProducer: Boolean,
    val createdAt: ZonedDateTime,
    var updatedAt: ZonedDateTime
) : Configurable {
    override fun updateConfiguration(config: Configurable): MessageProducer {
        if (config is MessageProducer) {
            this.defaultProducer = config.defaultProducer
            this.updatedAt = ZonedDateTime.now()
        }
        return this
    }

    companion object Factory {
        fun fromEntity(entity: MessageBrokerConfigurationEntity): MessageProducer {
            return MessageProducer(
                id = entity.id ?: throw IllegalArgumentException("BrokerTopic ID cannot be null"),
                producerName = entity.producerName,
                brokerType = entity.brokerType,
                keySerializationFormat = entity.keyFormat,
                valueSerializationFormat = entity.valueFormat,
                defaultProducer = entity.defaultProducer,
                createdAt = entity.createdAt,
                updatedAt = entity.updatedAt
            )
        }
    }
}
