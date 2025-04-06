package paladin.router.models.dispatch

import org.springframework.cloud.aws.messaging.core.QueueMessagingTemplate
import paladin.router.models.configuration.brokers.MessageBroker
import paladin.router.pojo.configuration.brokers.auth.SQSEncryptedConfig
import paladin.router.pojo.configuration.brokers.core.SQSBrokerConfig
import paladin.router.pojo.dispatch.MessageDispatcher

data class SQSDispatcher(
    override var broker: MessageBroker,
    override var config: SQSBrokerConfig,
    override var authConfig: SQSEncryptedConfig,
):MessageDispatcher() {

    private var producer: QueueMessagingTemplate? = null

    override fun <V> dispatch(topic: String, payload: V, schema: String?) {
        TODO("Not yet implemented")
    }

    override fun <K, V> dispatch(topic: String, key: K, payload: V, schema: String?) {
        TODO("Not yet implemented")
    }

    override fun build() {
        TODO("Not yet implemented")
    }

    override fun validate() {
        TODO("Not yet implemented")
    }
}