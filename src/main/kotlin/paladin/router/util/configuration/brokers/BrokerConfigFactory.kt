package paladin.router.util.configuration.brokers

import paladin.router.enums.configuration.BrokerType
import paladin.router.pojo.configuration.brokers.BrokerConfig
import paladin.router.pojo.configuration.brokers.KafkaBrokerConfig
import paladin.router.pojo.configuration.brokers.RabbitBrokerConfig
import paladin.router.pojo.configuration.brokers.SQSBrokerConfig


object BrokerConfigFactory {
    fun parseBrokerConfig(brokerType: BrokerType, properties: Map<String, Any>): BrokerConfig {
        return when (brokerType) {
            BrokerType.KAFKA -> KafkaBrokerConfig(
                bootstrapServers = properties["bootstrapServers"] as String,
                clientId = properties["clientId"] as String,
                groupId = properties["groupId"] as String?,
                enableAutoCommit = properties["enableAutoCommit"] as Boolean,
                autoCommitIntervalMs = properties["autoCommitIntervalMs"] as Int,
                requestTimeoutMs = properties["requestTimeoutMs"] as Int,
                retries = properties["retries"] as Int,
                acks = properties["acks"] as String
            )

            BrokerType.RABBIT -> RabbitBrokerConfig(
                host = properties["host"] as String,
                port = properties["port"] as Int,
                virtualHost = properties["virtualHost"] as String,
                exchangeName = properties["exchangeName"] as String?,
                queueName = properties["queueName"] as String?,
                prefetchCount = properties["prefetchCount"] as Int
            )

            BrokerType.SQS -> SQSBrokerConfig(
                region = properties["region"] as String,
                queueUrl = properties["queueUrl"] as String
            )

            else -> throw IllegalArgumentException("Broker type not supported")
        }
    }
}