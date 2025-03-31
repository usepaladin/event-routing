package paladin.router.models.dispatch

import org.springframework.amqp.rabbit.core.RabbitTemplate
import paladin.router.models.configuration.brokers.MessageBroker
import paladin.router.pojo.configuration.brokers.EncryptedBrokerAuthConfig
import paladin.router.pojo.configuration.brokers.RabbitBrokerConfig
import paladin.router.pojo.dispatch.MessageDispatcher

data class RabbitDispatcher(
    override val broker: MessageBroker,
    override val config: RabbitBrokerConfig,
    override val authConfig: EncryptedBrokerAuthConfig,
): MessageDispatcher<RabbitTemplate>() {

    private var producer: RabbitTemplate? = null

    override fun dispatch(payload: Any) {
        TODO()
    }

    override fun regenerateProducer() {
        TODO("Not yet implemented")
    }

    override fun validateProducerConfiguration() {
        TODO("Not yet implemented")
    }
}