package paladin.router.models.configuration.brokers.core

import paladin.router.enums.configuration.Broker
import paladin.router.util.Configurable

data class SQSBrokerConfig(
    override val brokerType: Broker.BrokerType = Broker.BrokerType.SQS,
    var queueUrl: String,
    var messageRetentionPeriod: Int = 345600,
    var visibilityTimeout: Int = 30,
    var maxNumberOfMessages: Int = 10
) : BrokerConfig {
    override fun updateConfiguration(config: Configurable): SQSBrokerConfig {
        if (config is SQSBrokerConfig) {
            queueUrl = config.queueUrl
            messageRetentionPeriod = config.messageRetentionPeriod
            visibilityTimeout = config.visibilityTimeout
            maxNumberOfMessages = config.maxNumberOfMessages
        }
        return this
    }
}