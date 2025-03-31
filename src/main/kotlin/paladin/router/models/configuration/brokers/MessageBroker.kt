package paladin.router.models.configuration.brokers

import paladin.router.entities.brokers.configuration.MessageBrokerConfigurationEntity

import paladin.router.enums.configuration.Broker
import paladin.router.pojo.configuration.brokers.EncryptedBrokerAuthConfig
import java.time.ZonedDateTime
import java.util.*

data class MessageBroker(
    val id: UUID,
    val brokerName: String,
    val brokerType: Broker.BrokerType,
    val brokerFormat: Broker.BrokerFormat,
    val defaultBroker: Boolean,
    val createdAt: ZonedDateTime,
    val updatedAt: ZonedDateTime
) {
    companion object {
        fun factory(entity: MessageBrokerConfigurationEntity, authConfig: EncryptedBrokerAuthConfig): MessageBroker {
            return MessageBroker(
                id = entity.id ?: throw IllegalArgumentException("BrokerTopic ID cannot be null"),
                brokerName = entity.brokerName,
                brokerType = entity.brokerType,
                brokerFormat = entity.brokerFormat,
                defaultBroker = entity.defaultBroker,
                createdAt = entity.createdAt,
                updatedAt = entity.updatedAt
            )
        }
    }
}
