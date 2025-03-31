package paladin.router.pojo.configuration.brokers

import paladin.router.enums.configuration.Broker

data class PulsarBrokerConfig(
    override val brokerType: Broker.BrokerType = Broker.BrokerType.PULSAR
): BrokerConfig