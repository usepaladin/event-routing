package paladin.router.models.configuration.brokers.core

import paladin.router.enums.configuration.Broker
import paladin.router.util.Configurable

data class RabbitBrokerConfig(
    override val brokerType: Broker.BrokerType = Broker.BrokerType.RABBIT,
    var host: String,
    var port: Int = 5672,
    var virtualHost: String = "/",
    var exchangeName: String?,
    var queueName: String?,
    var prefetchCount: Int = 10
) : BrokerConfig {
    override fun updateConfiguration(config: Configurable): RabbitBrokerConfig {
        if (config is RabbitBrokerConfig) {
            this.host = config.host
            this.port = config.port
            this.virtualHost = config.virtualHost
            this.exchangeName = config.exchangeName
            this.queueName = config.queueName
            this.prefetchCount = config.prefetchCount
        }
        return this
    }
}