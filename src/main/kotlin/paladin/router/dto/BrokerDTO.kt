package paladin.router.dto

import paladin.router.models.configuration.brokers.MessageProducer
import paladin.router.models.configuration.brokers.auth.EncryptedProducerConfig
import paladin.router.models.configuration.brokers.core.ProducerConfig

open class BrokerDTO(
    val broker: MessageProducer,
    val config: ProducerConfig,
    val authConfig: EncryptedProducerConfig,
){
    companion object Factory{
        fun fromEntity(broker: MessageProducer, config: ProducerConfig, authConfig: EncryptedProducerConfig) = BrokerDTO(
            broker = broker,
            config = config,
            authConfig = authConfig,
        )
    }
}