package paladin.router.util.factory

import paladin.router.models.configuration.brokers.MessageBroker
import paladin.router.pojo.configuration.brokers.*
import paladin.router.pojo.dispatch.MessageDispatcher

object MessageDispatcherFactory {

    fun fromBrokerConfig(broker: MessageBroker, config: BrokerConfig): MessageDispatcher{
        when(config){
            is SQSBrokerConfig -> {
                TODO()
            }
            is KafkaBrokerConfig -> {
                TODO()
            }
            is RabbitBrokerConfig -> {
                TODO()
            }
            is MQTTBrokerConfig -> {
                TODO()
            }
            is PulsarBrokerConfig -> {
                TODO()
            }
            else -> {
                throw IllegalArgumentException("Unsupported broker config type")
            }
        }
    }
}