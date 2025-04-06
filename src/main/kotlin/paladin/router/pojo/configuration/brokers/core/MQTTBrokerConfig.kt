package paladin.router.pojo.configuration.brokers.core

import paladin.router.enums.configuration.Broker
import paladin.router.util.factory.Configurable

data class MQTTBrokerConfig(
    override val brokerType: Broker.BrokerType = Broker.BrokerType.MQTT,
): BrokerConfig{
    override fun updateConfiguration(config: Configurable): MQTTBrokerConfig {
        if (config is MQTTBrokerConfig) {
            TODO()
        }
        return this
    }
}