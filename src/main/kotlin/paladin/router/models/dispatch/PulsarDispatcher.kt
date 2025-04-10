package paladin.router.models.dispatch

import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.pulsar.core.PulsarTemplate
import paladin.router.models.configuration.brokers.MessageBroker
import paladin.router.pojo.configuration.brokers.auth.PulsarEncryptedConfig
import paladin.router.pojo.configuration.brokers.core.PulsarBrokerConfig
import paladin.router.pojo.dispatch.MessageDispatcher
import paladin.router.services.schema.SchemaService

data class PulsarDispatcher<T>(
    override val broker: MessageBroker,
    override val config: PulsarBrokerConfig,
    override val authConfig: PulsarEncryptedConfig,
    override val schemaService: SchemaService
): MessageDispatcher() {

    private var producer: PulsarTemplate<T>? = null

    override val logger: KLogger
        get() = KotlinLogging.logger {  }

    override fun <K, V> dispatch(topic: String, key: K, payload: V, keySchema: String?, payloadSchema: String?) {
        TODO("Not yet implemented")
    }

    override fun <V> dispatch(topic: String, payload: V, payloadSchema: String?) {
        TODO("Not yet implemented")
    }

    override fun testConnection() {
        TODO("Not yet implemented")
    }

    override fun build() {
        TODO("Not yet implemented")
    }

    override fun validate() {
        TODO("Not yet implemented")
    }
}