package paladin.router.pojo.configuration.brokers.core

import paladin.router.enums.configuration.Broker
import paladin.router.util.factory.Configurable

data class PulsarBrokerConfig(
    override val brokerType: Broker.BrokerType = Broker.BrokerType.PULSAR
): BrokerConfig, Configurable {
    override fun updateConfiguration(config: Configurable): PulsarBrokerConfig {
        if (config is PulsarBrokerConfig) {
            TODO()
        }
        return this
    }
}