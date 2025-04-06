package paladin.router.models.dispatch

import org.springframework.amqp.rabbit.core.RabbitTemplate
import paladin.router.models.configuration.brokers.MessageBroker
import paladin.router.pojo.configuration.brokers.auth.RabbitEncryptedConfig
import paladin.router.pojo.configuration.brokers.core.RabbitBrokerConfig
import paladin.router.pojo.dispatch.MessageDispatcher

data class RabbitDispatcher(
    override val broker: MessageBroker,
    override val config: RabbitBrokerConfig,
    override val authConfig: RabbitEncryptedConfig,
): MessageDispatcher() {

    private var producer: RabbitTemplate? = null

    override fun <K, V> dispatch(topic: String, key: K, payload: V, schema: String?) {
        TODO("Not yet implemented")
    }

    override fun <V> dispatch(topic: String, payload: V, schema: String?) {
        TODO("Not yet implemented")
    }

    override fun build() {
        TODO("Not yet implemented")
    }

    override fun validate() {
        TODO("Not yet implemented")
    }
}