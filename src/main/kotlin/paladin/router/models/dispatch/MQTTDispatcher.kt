package paladin.router.models.dispatch

import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import paladin.router.models.configuration.brokers.MessageBroker
import paladin.router.pojo.configuration.brokers.auth.MQTTEncryptedConfig
import paladin.router.pojo.configuration.brokers.core.MQTTBrokerConfig
import paladin.router.pojo.dispatch.MessageDispatcher
import paladin.router.services.schema.SchemaService

data class MQTTDispatcher(
    override val broker: MessageBroker,
    override val config: MQTTBrokerConfig,
    override val authConfig: MQTTEncryptedConfig,
    override val schemaService: SchemaService
):MessageDispatcher(){

    override val logger: KLogger
        get() = KotlinLogging.logger {  }

    //todo => Research and create MQTT Producer
    private var producer: Any? = null

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