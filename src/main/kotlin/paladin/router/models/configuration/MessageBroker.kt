package paladin.router.models.configuration

import paladin.router.entities.brokers.MessageBrokerConfigurationEntity
import paladin.router.enums.configuration.BrokerFormat
import paladin.router.enums.configuration.BrokerType
import paladin.router.pojo.configuration.brokers.BrokerConfig
import paladin.router.pojo.configuration.brokers.EncryptedBrokerAuthConfig
import paladin.router.util.configuration.brokers.BrokerConfigFactory
import java.time.ZonedDateTime
import java.util.*

data class MessageBroker(
    val id: UUID,
    val brokerName: String,
    val brokerType: BrokerType,
    val brokerFormat: BrokerFormat,
    val defaultBroker: Boolean,
    val brokerConfig: BrokerConfig,
    val authConfig: EncryptedBrokerAuthConfig,
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
                brokerConfig = BrokerConfigFactory.parseBrokerConfig(entity.brokerType, entity.brokerConfig),
                updatedAt = entity.updatedAt,
                authConfig = authConfig
            )
        }
    }
}