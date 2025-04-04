package paladin.router.pojo.dispatch

import paladin.router.models.configuration.brokers.MessageBroker
import paladin.router.pojo.configuration.brokers.core.BrokerConfig
import paladin.router.pojo.configuration.brokers.auth.EncryptedBrokerConfig

abstract class MessageDispatcher{
    abstract val broker: MessageBroker
    abstract val config: BrokerConfig
    abstract val authConfig: EncryptedBrokerConfig

    /**
     * Uses the provided broker to dispatch a message to the appropriate destination within the brokers reach.
     */
    abstract fun <K, V> dispatch(key: K, payload: V)

    /**
     * Builds the dispatcher of the message broker from the configuration properties
     * provided by the user
     */
    abstract fun build()

    /**
     * Validates the dispatcher of the message broker,
     * ensuring all configuration properties required for successful operation are present
     * before the dispatcher is built and saved for use.
     */
    abstract fun validate()
}