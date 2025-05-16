package paladin.router.models.configuration.brokers.core

import paladin.router.enums.configuration.Broker
import paladin.router.util.Configurable

data class RabbitProducerConfig(
    override val brokerType: Broker.BrokerType = Broker.BrokerType.RABBIT,
    var exchangeName: String?,
    var queueName: String?,
    var prefetchCount: Int = 10,
    var publisherReturns: Boolean = true,
) : ProducerConfig {
    override fun updateConfiguration(config: Configurable): RabbitProducerConfig {
        if (config is RabbitProducerConfig) {
            this.exchangeName = config.exchangeName
            this.queueName = config.queueName
            this.prefetchCount = config.prefetchCount
            this.publisherReturns = config.publisherReturns
        }
        return this
    }
}