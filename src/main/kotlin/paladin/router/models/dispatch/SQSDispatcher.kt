package paladin.router.models.dispatch

import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.cloud.aws.messaging.core.QueueMessagingTemplate
import paladin.router.models.configuration.brokers.MessageBroker
import paladin.router.pojo.configuration.brokers.auth.SQSEncryptedConfig
import paladin.router.pojo.configuration.brokers.core.SQSBrokerConfig
import paladin.router.pojo.dispatch.MessageDispatcher
import paladin.router.services.schema.SchemaService

data class SQSDispatcher(
    override var broker: MessageBroker,
    override var config: SQSBrokerConfig,
    override var authConfig: SQSEncryptedConfig,
    override val schemaService: SchemaService
):MessageDispatcher() {

    private var producer: QueueMessagingTemplate? = null

    override val logger: KLogger
        get() = KotlinLogging.logger {  }

    override fun <V> dispatch(topic: String, payload: V, payloadSchema: String?) {
        TODO("Not yet implemented")
    }

    override fun <K, V> dispatch(topic: String, key: K, payload: V, keySchema: String?, payloadSchema: String?) {
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