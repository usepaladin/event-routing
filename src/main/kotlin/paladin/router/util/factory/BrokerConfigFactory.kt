package paladin.router.util.factory

import paladin.router.enums.configuration.Broker
import paladin.router.enums.configuration.SQS.Region
import paladin.router.pojo.configuration.brokers.auth.EncryptedBrokerConfig
import paladin.router.pojo.configuration.brokers.auth.KafkaEncryptedConfig
import paladin.router.pojo.configuration.brokers.auth.RabbitEncryptedConfig
import paladin.router.pojo.configuration.brokers.auth.SQSEncryptedConfig
import paladin.router.pojo.configuration.brokers.core.BrokerConfig
import paladin.router.pojo.configuration.brokers.core.KafkaBrokerConfig
import paladin.router.pojo.configuration.brokers.core.RabbitBrokerConfig
import paladin.router.pojo.configuration.brokers.core.SQSBrokerConfig


object BrokerConfigFactory {
    fun fromConfigurationProperties(brokerType: Broker.BrokerType, properties: Map<String, Any>): Pair<BrokerConfig, EncryptedBrokerConfig> {
        return when (brokerType) {
            Broker.BrokerType.KAFKA -> parseKafkaConfiguration(properties)
            Broker.BrokerType.RABBIT -> parseRabbitConfiguration(properties)
            Broker.BrokerType.SQS -> parseSQSConfiguration(properties)

            else -> throw IllegalArgumentException("Broker type not supported")
        }
    }

    private fun parseKafkaConfiguration(properties: Map<String, Any>): Pair<KafkaBrokerConfig, KafkaEncryptedConfig> {
        val brokerConfig = KafkaBrokerConfig(
            clientId = properties["clientId"] as String,
            groupId = properties["groupId"] as String?,
            enableAutoCommit = properties["enableAutoCommit"] as Boolean,
            autoCommitIntervalMs = properties["autoCommitIntervalMs"] as Int,
            requestTimeoutMs = properties["requestTimeoutMs"] as Int,
            retries = properties["retries"] as Int,
            acks = properties["acks"] as String
        )

        val encryptedConfig = KafkaEncryptedConfig(
            bootstrapServers = properties["bootstrapServers"] as String,
            //todo add other properties
        )

        return Pair(brokerConfig, encryptedConfig)
    }

    private fun parseRabbitConfiguration(properties: Map<String, Any>): Pair<RabbitBrokerConfig, RabbitEncryptedConfig> {
        val brokerConfig = RabbitBrokerConfig(
            host = properties["host"] as String,
            port = properties["port"] as Int,
            virtualHost = properties["virtualHost"] as String,
            exchangeName = properties["exchangeName"] as String?,
            queueName = properties["queueName"] as String?,
            prefetchCount = properties["prefetchCount"] as Int
        )

        val encryptedConfig = RabbitEncryptedConfig(
            addresses = properties["addresses"] as String,
        )

        return Pair(brokerConfig, encryptedConfig)
    }

    private fun parseSQSConfiguration(properties: Map<String, Any>): Pair<SQSBrokerConfig, SQSEncryptedConfig> {
        val brokerConfig = SQSBrokerConfig(
            queueUrl = properties["queueUrl"] as String,
            messageRetentionPeriod = properties["messageRetentionPeriod"] as Int,
            visibilityTimeout = properties["visibilityTimeout"] as Int,
            maxNumberOfMessages = properties["maxNumberOfMessages"] as Int
        )

        val encryptedConfig = SQSEncryptedConfig(
            accessKey = properties["accessKeyId"] as String,
            secretKey = properties["secretAccessKey"] as String,
            region = Region.fromRegion(properties["region"] as String) ?: throw IllegalArgumentException("Invalid region")

        )

        return Pair(brokerConfig, encryptedConfig)
    }
}