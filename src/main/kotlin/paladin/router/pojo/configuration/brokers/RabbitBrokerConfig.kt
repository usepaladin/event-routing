package paladin.router.pojo.configuration.brokers

import paladin.router.enums.configuration.BrokerType

data class RabbitBrokerConfig(
    override val brokerType: BrokerType = BrokerType.RABBIT,
    val host: String,
    val port: Int = 5672,
    val virtualHost: String = "/",
    val exchangeName: String?,
    val queueName: String?,
    val prefetchCount: Int = 10
) : BrokerConfig