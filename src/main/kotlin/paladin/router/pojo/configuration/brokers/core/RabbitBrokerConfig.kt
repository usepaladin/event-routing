package paladin.router.pojo.configuration.brokers.core

import paladin.router.enums.configuration.Broker

data class RabbitBrokerConfig(
    override val brokerType: Broker.BrokerType = Broker.BrokerType.RABBIT,
    val host: String,
    val port: Int = 5672,
    val virtualHost: String = "/",
    val exchangeName: String?,
    val queueName: String?,
    val prefetchCount: Int = 10
) : BrokerConfig