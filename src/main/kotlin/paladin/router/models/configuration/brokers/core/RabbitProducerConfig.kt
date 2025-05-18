package paladin.router.models.configuration.brokers.core

import paladin.router.enums.configuration.Broker
import paladin.router.util.Configurable

data class RabbitProducerConfig(
    override val brokerType: Broker.BrokerType = Broker.BrokerType.RABBIT,
    override var allowRetries: Boolean = true,
    override var allowAsync: Boolean = true,
    override var retryMaxAttempts: Int = 3,
    override var retryBackoff: Long = 1000L,
    override var connectionTimeout: Long = 10000L,
    override var errorHandlerStrategy: ProducerConfig.ErrorStrategy = ProducerConfig.ErrorStrategy.DLQ,
    var exchangeName: String?,
    var queueName: String?,
    var prefetchCount: Int = 10,
    var publisherReturns: Boolean = true,
    var sync: Boolean = false,
    var channelCacheSize: Int = 25,
    var defaultRoutingKey: String? = null,
) : ProducerConfig {
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