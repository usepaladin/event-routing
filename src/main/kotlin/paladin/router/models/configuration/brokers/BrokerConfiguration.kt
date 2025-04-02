package paladin.router.models.configuration.brokers

import paladin.router.entities.brokers.configuration.MessageBrokerConfigurationEntity
import paladin.router.pojo.configuration.brokers.BrokerConfig
import paladin.router.pojo.configuration.brokers.EncryptedBrokerAuthConfig
import paladin.router.util.factory.BrokerConfigFactory

class BrokerConfiguration (
    val brokerConfig: BrokerConfig,
    val authConfig: EncryptedBrokerAuthConfig,
    val broker: MessageBroker
)  {
    companion object {
        fun factory(broker: MessageBroker, entity: MessageBrokerConfigurationEntity, authConfig: EncryptedBrokerAuthConfig): BrokerConfiguration {
            return BrokerConfiguration(
                broker = broker,
                brokerConfig = BrokerConfigFactory.parseBrokerConfig(entity.brokerType, entity.brokerConfig),
                authConfig = authConfig
            )
        }
    }
}