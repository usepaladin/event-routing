package paladin.router.models.dispatch

import paladin.router.models.configuration.brokers.MessageBroker
import paladin.router.pojo.configuration.brokers.auth.MQTTEncryptedConfig
import paladin.router.pojo.configuration.brokers.core.MQTTBrokerConfig
import paladin.router.pojo.dispatch.MessageDispatcher

data class MQTTDispatcher(
    override val broker: MessageBroker,
    override val config: MQTTBrokerConfig,
    override val authConfig: MQTTEncryptedConfig,
):MessageDispatcher(){

    //todo => Research and create MQTT Producer
    private var producer: Any? = null

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