package paladin.router.util.factory

import paladin.router.enums.configuration.Broker
import paladin.router.enums.configuration.sqs.Region
import paladin.router.models.configuration.brokers.auth.EncryptedProducerConfig
import paladin.router.models.configuration.brokers.auth.KafkaEncryptedConfig
import paladin.router.models.configuration.brokers.auth.RabbitEncryptedConfig
import paladin.router.models.configuration.brokers.auth.SQSEncryptedConfig
import paladin.router.models.configuration.brokers.core.KafkaProducerConfig
import paladin.router.models.configuration.brokers.core.ProducerConfig
import paladin.router.models.configuration.brokers.core.RabbitProducerConfig
import paladin.router.models.configuration.brokers.core.SQSProducerConfig


object BrokerConfigFactory {
    fun fromConfigurationProperties(
        brokerType: Broker.BrokerType,
        properties: Map<String, Any>
    ): Pair<ProducerConfig, EncryptedProducerConfig> {
        return when (brokerType) {
            Broker.BrokerType.KAFKA -> parseKafkaConfiguration(properties)
            Broker.BrokerType.RABBIT -> parseRabbitConfiguration(properties)
            Broker.BrokerType.SQS -> parseSQSConfiguration(properties)
        }
    }

    private fun parseKafkaConfiguration(properties: Map<String, Any>): Pair<KafkaProducerConfig, KafkaEncryptedConfig> {
        val brokerConfig = KafkaProducerConfig(
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
        )

        properties["schemaRegistryUrl"]?.let {
            encryptedConfig.apply {
                schemaRegistryUrl = it as String
            }
        }

        return Pair(brokerConfig, encryptedConfig)
    }

    private fun parseRabbitConfiguration(properties: Map<String, Any>): Pair<RabbitProducerConfig, RabbitEncryptedConfig> {
        val brokerConfig = RabbitProducerConfig(
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

    private fun parseSQSConfiguration(properties: Map<String, Any>): Pair<SQSProducerConfig, SQSEncryptedConfig> {
        val brokerConfig = SQSProducerConfig(
            queueUrl = properties["queueUrl"] as String,
            messageRetentionPeriod = properties["messageRetentionPeriod"] as Int,
            visibilityTimeout = properties["visibilityTimeout"] as Int,
            maxNumberOfMessages = properties["maxNumberOfMessages"] as Int
        )

        val encryptedConfig = SQSEncryptedConfig(
            accessKey = properties["accessKeyId"] as String,
            secretKey = properties["secretAccessKey"] as String,
            region = Region.fromRegion(properties["region"] as String)
                ?: throw IllegalArgumentException("Invalid region")

        )

        return Pair(brokerConfig, encryptedConfig)
    }
}