package paladin.router.models.configuration.producers.core

import paladin.router.enums.configuration.Broker
import paladin.router.util.Configurable

data class SQSProducerConfig(
    override val brokerType: Broker.BrokerType = Broker.BrokerType.SQS,
    override var allowAsync: Boolean = true,
    override var retryMaxAttempts: Int = 3,
    override var retryBackoff: Long = 1000L,
    override var connectionTimeout: Long = 10000L,
    override var errorHandlerStrategy: ProducerConfig.ErrorStrategy = ProducerConfig.ErrorStrategy.DLQ,
    var queueUrl: String,
    var defaultGroupId: String? = null,
    var messageRetentionPeriod: Int = 345600,
    var visibilityTimeout: Int = 30,
    var maxNumberOfMessages: Int = 10
) : ProducerConfig {
    override fun updateConfiguration(config: Configurable): SQSProducerConfig {
        if (config is SQSProducerConfig) {
            super.updateConfiguration(config)
            queueUrl = config.queueUrl
            messageRetentionPeriod = config.messageRetentionPeriod
            visibilityTimeout = config.visibilityTimeout
            maxNumberOfMessages = config.maxNumberOfMessages
        }
        return this
    }
}