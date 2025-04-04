package paladin.router.models.dispatch

import org.springframework.pulsar.core.PulsarTemplate
import paladin.router.models.configuration.brokers.MessageBroker
import paladin.router.pojo.configuration.brokers.auth.PulsarEncryptedConfig
import paladin.router.pojo.configuration.brokers.core.PulsarBrokerConfig
import paladin.router.pojo.dispatch.MessageDispatcher

data class PulsarDispatcher<T>(
    override val broker: MessageBroker,
    override val config: PulsarBrokerConfig,
    override val authConfig: PulsarEncryptedConfig,
): MessageDispatcher() {

    private var producer: PulsarTemplate<T>? = null

    override fun <K, V> dispatch(key: K, payload: V) {
        TODO("Not yet implemented")
    }

    override fun build() {
        TODO("Not yet implemented")
    }

    override fun validate() {
        TODO("Not yet implemented")
    }
}