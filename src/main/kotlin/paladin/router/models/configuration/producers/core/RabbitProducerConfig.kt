package paladin.router.models.configuration.producers.core

import paladin.router.enums.configuration.Broker
import paladin.router.util.Configurable

data class RabbitProducerConfig(
    override val brokerType: Broker.BrokerType = Broker.BrokerType.RABBIT,
    override var allowAsync: Boolean = true,
    override var requireKey: Boolean = false,
    override var retryMaxAttempts: Int = 3,
    override var retryBackoff: Int = 1000,
    override var connectionTimeout: Int = 10000,
    override var errorHandlerStrategy: ProducerConfig.ErrorStrategy = ProducerConfig.ErrorStrategy.DLQ,
    var exchangeName: String?,
    var queueName: String?,
    var prefetchCount: Int = 10,
    var publisherReturns: Boolean = true,
    var sync: Boolean = false,
    var channelCacheSize: Int = 25,
    var defaultRoutingKey: String? = null,
) : ProducerConfig {
    init {
        require(exchangeName != null || queueName != null) {
            "Either exchangeName or queueName must be provided"
        }
    }

    override fun updateConfiguration(config: Configurable): RabbitProducerConfig {
        if (config is RabbitProducerConfig) {
            super.updateConfiguration(config)
            this.exchangeName = config.exchangeName
            this.queueName = config.queueName
            this.prefetchCount = config.prefetchCount
            this.publisherReturns = config.publisherReturns
            this.sync = config.sync
            this.channelCacheSize = config.channelCacheSize
            this.defaultRoutingKey = config.defaultRoutingKey
        }
        return this
    }
}