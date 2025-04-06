package paladin.router.models.dispatch

import org.apache.kafka.clients.producer.KafkaProducer
import paladin.router.models.configuration.brokers.MessageBroker
import paladin.router.pojo.configuration.brokers.auth.KafkaEncryptedConfig
import paladin.router.pojo.configuration.brokers.core.BrokerConfig
import paladin.router.pojo.configuration.brokers.core.KafkaBrokerConfig
import paladin.router.pojo.dispatch.MessageDispatcher

data class KafkaDispatcher <T, V>(
    override val broker: MessageBroker,
    override val config: KafkaBrokerConfig,
    override val authConfig: KafkaEncryptedConfig,
): MessageDispatcher()  {

    private var producer: KafkaProducer<T, V>? = null

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