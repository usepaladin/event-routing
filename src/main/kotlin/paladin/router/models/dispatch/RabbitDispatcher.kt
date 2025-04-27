package paladin.router.models.dispatch

import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.amqp.rabbit.core.RabbitTemplate
import paladin.router.models.configuration.brokers.MessageBroker
import paladin.router.models.configuration.brokers.auth.RabbitEncryptedConfig
import paladin.router.models.configuration.brokers.core.RabbitBrokerConfig
import paladin.router.services.schema.SchemaService

data class RabbitDispatcher(
    override val broker: MessageBroker,
    override val config: RabbitBrokerConfig,
    override val authConfig: RabbitEncryptedConfig,
    override val schemaService: SchemaService
): MessageDispatcher() {

    private var producer: RabbitTemplate? = null

    override val logger: KLogger
        get() = KotlinLogging.logger {  }

    override fun <K, V> dispatch(topic: String, key: K, payload: V, keySchema: String?, payloadSchema: String?) {
        TODO("Not yet implemented")
    }

    override fun testConnection() {
        TODO("Not yet implemented")
    }

    override fun <V> dispatch(topic: String, payload: V, payloadSchema: String?) {
        TODO("Not yet implemented")
    }

    override fun build() {
        TODO("Not yet implemented")
    }

    override fun validate() {
        TODO("Not yet implemented")
    }
}