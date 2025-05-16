package paladin.router.models.dispatch

import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.cloud.aws.messaging.core.QueueMessagingTemplate
import paladin.router.models.configuration.brokers.MessageProducer
import paladin.router.models.configuration.brokers.auth.SQSEncryptedConfig
import paladin.router.models.configuration.brokers.core.SQSProducerConfig
import paladin.router.services.schema.SchemaService

data class SQSDispatcher(
    override var broker: MessageProducer,
    override var config: SQSProducerConfig,
    override var authConfig: SQSEncryptedConfig,
    override val schemaService: SchemaService
) : MessageDispatcher() {

    private var producer: QueueMessagingTemplate? = null

    override val logger: KLogger
        get() = KotlinLogging.logger { }

    override fun <V> dispatch(payload: V, topic: DispatchTopic) {
        TODO("Not yet implemented")
    }

    override fun <K, V> dispatch(key: K, payload: V, topic: DispatchTopic) {
        //NO-OP
        throw NotImplementedError("SQS does not support key-value dispatching")
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