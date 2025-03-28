package paladin.router.pojo.configuration.brokers

import paladin.router.enums.configuration.BrokerType

data class SQSBrokerConfig(
    override val brokerType: BrokerType = BrokerType.SQS,
    val queueUrl: String,
    val region: String,
    val messageRetentionPeriod: Int = 345600,
    val visibilityTimeout: Int = 30,
    val maxNumberOfMessages: Int = 10
) : BrokerConfig