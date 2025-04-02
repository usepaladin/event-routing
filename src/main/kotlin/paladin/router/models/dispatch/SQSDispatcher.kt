package paladin.router.models.dispatch

import org.springframework.cloud.aws.messaging.core.QueueMessagingTemplate
import paladin.router.models.configuration.brokers.MessageBroker
import paladin.router.pojo.configuration.brokers.EncryptedBrokerAuthConfig
import paladin.router.pojo.configuration.brokers.SQSBrokerConfig
import paladin.router.pojo.dispatch.MessageDispatcher

data class SQSDispatcher(
    override val broker: MessageBroker,
    override val config: SQSBrokerConfig,
    override val authConfig: EncryptedBrokerAuthConfig,
):MessageDispatcher() {

    private var producer: QueueMessagingTemplate? = null

    override fun dispatch(payload: Any) {
        TODO("Not yet implemented")
    }

    override fun build() {
        TODO("Not yet implemented")
    }

    override fun validate() {
        TODO("Not yet implemented")
    }
}