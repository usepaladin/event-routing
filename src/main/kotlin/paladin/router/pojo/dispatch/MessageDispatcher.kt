package paladin.router.pojo.dispatch

import paladin.router.models.configuration.brokers.MessageBroker
import paladin.router.pojo.configuration.brokers.BrokerConfig
import paladin.router.pojo.configuration.brokers.EncryptedBrokerAuthConfig

abstract class MessageDispatcher{
    abstract val broker: MessageBroker
    abstract val config: BrokerConfig
    abstract val authConfig: EncryptedBrokerAuthConfig
    abstract fun dispatch(payload: Any)
    abstract fun regenerateProducer()
    abstract fun validateProducerConfiguration()
}