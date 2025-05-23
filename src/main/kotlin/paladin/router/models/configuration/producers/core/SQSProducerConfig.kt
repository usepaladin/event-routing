package paladin.router.models.configuration.producers.core

import paladin.router.enums.configuration.Broker
import paladin.router.util.Configurable

data class SQSProducerConfig(
    override val brokerType: Broker.BrokerType = Broker.BrokerType.SQS,
    override var allowAsync: Boolean = true,
    override var requireKey: Boolean = false,
    override var retryMaxAttempts: Int = 3,
    override var retryBackoff: Int = 1000,
    override var connectionTimeout: Int = 10000,
    override var errorHandlerStrategy: ProducerConfig.ErrorStrategy = ProducerConfig.ErrorStrategy.DLQ,
    var queueUrl: String,
    var fifoQueue: Boolean = false,
    var defaultGroupId: String? = null,
    var messageRetentionPeriod: Int = 345600,
    var visibilityTimeout: Int = 30,
    var maxNumberOfMessages: Int = 10
) : ProducerConfig {
    init {
        require(queueUrl.isNotBlank()) { "Queue URL cannot be blank" }
        require(queueUrl.startsWith("https://sqs.") || queueUrl.startsWith("http://")) {
            "Invalid SQS queue URL format"
        }
    }

    override fun updateConfiguration(config: Configurable): SQSProducerConfig {
        if (config is SQSProducerConfig) {
            super.updateConfiguration(config)
            queueUrl = config.queueUrl
            messageRetentionPeriod = config.messageRetentionPeriod
            visibilityTimeout = config.visibilityTimeout
            maxNumberOfMessages = config.maxNumberOfMessages
            fifoQueue = config.fifoQueue
            defaultGroupId = config.defaultGroupId
        }
        return this
    }
}