package paladin.router.models.configuration.brokers.core

import paladin.router.enums.configuration.Broker
import paladin.router.util.Configurable

data class RabbitProducerConfig(
    override val brokerType: Broker.BrokerType = Broker.BrokerType.RABBIT,
    var exchangeName: String?,
    var queueName: String?,
    var prefetchCount: Int = 10,
    var publisherReturns: Boolean = true,
    var sync: Boolean = false,
    var allowRetries: Boolean = false,
    var retryMaxAttempts: Int = 3,
    var retryBackoff: Long = 1000L,
    var connectionTimeout: Long = 10000L,
    var channelCacheSize: Int = 25,
    var defaultRoutingKey: String? = null,
) : ProducerConfig {
    override fun updateConfiguration(config: Configurable): RabbitProducerConfig {
        if (config is RabbitProducerConfig) {
            this.exchangeName = config.exchangeName
            this.queueName = config.queueName
            this.prefetchCount = config.prefetchCount
            this.publisherReturns = config.publisherReturns
            this.sync = config.sync
            this.allowRetries = config.allowRetries
            this.retryMaxAttempts = config.retryMaxAttempts
            this.retryBackoff = config.retryBackoff
            this.connectionTimeout = config.connectionTimeout
            this.channelCacheSize = config.channelCacheSize
            this.defaultRoutingKey = config.defaultRoutingKey
        }
        return this
    }
}