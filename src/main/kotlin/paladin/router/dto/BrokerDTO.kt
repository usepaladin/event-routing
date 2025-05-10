package paladin.router.dto

import paladin.router.models.configuration.brokers.MessageBroker
import paladin.router.models.configuration.brokers.auth.EncryptedBrokerConfig
import paladin.router.models.configuration.brokers.core.BrokerConfig

open class BrokerDTO(
    val broker: MessageBroker,
    val config: BrokerConfig,
    val authConfig: EncryptedBrokerConfig,
){
    companion object Factory{
        fun fromEntity(broker: MessageBroker, config: BrokerConfig, authConfig: EncryptedBrokerConfig) = BrokerDTO(
            broker = broker,
            config = config,
            authConfig = authConfig,
        )
    }
}