package paladin.router.pojo.configuration.brokers

import paladin.router.enums.configuration.Broker

data class SQSBrokerConfig(
    override val brokerType: Broker.BrokerType = Broker.BrokerType.SQS,
    val queueUrl: String,
    val region: String,
    val messageRetentionPeriod: Int = 345600,
    val visibilityTimeout: Int = 30,
    val maxNumberOfMessages: Int = 10
) : BrokerConfig