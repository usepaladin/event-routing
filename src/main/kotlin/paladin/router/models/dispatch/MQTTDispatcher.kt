package paladin.router.models.dispatch

import paladin.router.models.configuration.brokers.MessageBroker
import paladin.router.pojo.configuration.brokers.BrokerConfig
import paladin.router.pojo.configuration.brokers.EncryptedBrokerAuthConfig
import paladin.router.pojo.configuration.brokers.MQTTBrokerConfig
import paladin.router.pojo.dispatch.MessageDispatcher

data class MQTTDispatcher(
    override val broker: MessageBroker,
    override val config: MQTTBrokerConfig,
    override val authConfig: EncryptedBrokerAuthConfig,
):MessageDispatcher(){

    //todo => Research and create MQTT Producer
    private var producer: Any? = null

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