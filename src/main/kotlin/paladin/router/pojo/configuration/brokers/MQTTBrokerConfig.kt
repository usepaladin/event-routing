package paladin.router.pojo.configuration.brokers

import paladin.router.enums.configuration.Broker

data class MQTTBrokerConfig(
    override val brokerType: Broker.BrokerType = Broker.BrokerType.MQTT,
): BrokerConfig