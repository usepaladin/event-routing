package paladin.router.models.dispatch

import org.apache.kafka.clients.producer.KafkaProducer
import org.springframework.pulsar.core.PulsarTemplate
import paladin.router.models.configuration.brokers.MessageBroker
import paladin.router.pojo.configuration.brokers.EncryptedBrokerAuthConfig
import paladin.router.pojo.configuration.brokers.PulsarBrokerConfig
import paladin.router.pojo.dispatch.MessageDispatcher

data class PulsarDispatcher<T>(
    override val broker: MessageBroker,
    override val config: PulsarBrokerConfig,
    override val authConfig: EncryptedBrokerAuthConfig,
): MessageDispatcher() {

    private var producer: PulsarTemplate<T>? = null

    override fun dispatch(payload: Any) {
        TODO("Not yet implemented")
    }

    override fun regenerateProducer() {
        TODO("Not yet implemented")
    }

    override fun validateProducerConfiguration() {
        TODO("Not yet implemented")
    }
}