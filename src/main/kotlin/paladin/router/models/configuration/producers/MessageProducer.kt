package paladin.router.models.configuration.producers

import paladin.router.entities.brokers.configuration.MessageProducerConfigurationEntity

import paladin.router.enums.configuration.Broker
import paladin.router.util.Configurable
import java.time.ZonedDateTime
import java.util.*

data class MessageProducer(
    val id: UUID,
    var producerName: String,
    val brokerType: Broker.BrokerType,
    var valueSerializationFormat: Broker.ProducerFormat,
    var keySerializationFormat: Broker.ProducerFormat?,
    val createdAt: ZonedDateTime,
    var updatedAt: ZonedDateTime
) : Configurable {
    override fun updateConfiguration(config: Configurable): MessageProducer {
        if (config is MessageProducer) {
            this.updatedAt = ZonedDateTime.now()
            this.producerName = config.producerName

            // Todo: Figure out how to rebuild producers if the serialization format changes
            this.valueSerializationFormat = config.valueSerializationFormat
            this.keySerializationFormat = config.keySerializationFormat
        }
        return this
    }

    companion object Factory {
        fun fromEntity(entity: MessageProducerConfigurationEntity): MessageProducer {
            return MessageProducer(
                id = entity.id ?: throw IllegalArgumentException("BrokerTopic ID cannot be null"),
                producerName = entity.producerName,
                brokerType = entity.brokerType,
                keySerializationFormat = entity.keyFormat,
                valueSerializationFormat = entity.valueFormat,
                createdAt = entity.createdAt,
                updatedAt = entity.updatedAt
            )
        }
    }
}
