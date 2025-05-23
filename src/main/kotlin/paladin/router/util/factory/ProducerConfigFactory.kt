package paladin.router.util.factory

import paladin.router.enums.configuration.Broker
import paladin.router.models.configuration.producers.auth.EncryptedProducerConfig
import paladin.router.models.configuration.producers.auth.KafkaEncryptedConfig
import paladin.router.models.configuration.producers.auth.RabbitEncryptedConfig
import paladin.router.models.configuration.producers.auth.SQSEncryptedConfig
import paladin.router.models.configuration.producers.core.KafkaProducerConfig
import paladin.router.models.configuration.producers.core.ProducerConfig
import paladin.router.models.configuration.producers.core.RabbitProducerConfig
import paladin.router.models.configuration.producers.core.SQSProducerConfig
import software.amazon.awssdk.regions.Region


object ProducerConfigFactory {
    fun fromConfigurationProperties(
        brokerType: Broker.BrokerType,
        properties: Map<String, Any?>
    ): Pair<ProducerConfig, EncryptedProducerConfig> {
        return when (brokerType) {
            Broker.BrokerType.KAFKA -> parseKafkaConfiguration(properties)
            Broker.BrokerType.RABBIT -> parseRabbitConfiguration(properties)
            Broker.BrokerType.SQS -> parseSQSConfiguration(properties)
        }
    }

    private fun parseKafkaConfiguration(
        properties: Map<String, Any?>,
    ): Pair<KafkaProducerConfig, KafkaEncryptedConfig> {
        val brokerConfig = KafkaProducerConfig(
            clientId = properties["clientId"] as String,
            groupId = properties["groupId"] as String?,
            allowAsync = properties["allowAsync"] as Boolean,
            retryMaxAttempts = properties["retryMaxAttempts"] as Int,
            retryBackoff = properties["retryBackoff"] as Int,
            connectionTimeout = properties["connectionTimeout"] as Int,
            errorHandlerStrategy = ProducerConfig.ErrorStrategy.valueOf(
                (properties["errorHandlerStrategy"] as String?)?.uppercase() ?: ProducerConfig.ErrorStrategy.DLQ.name
            ),
            enableAutoCommit = properties["enableAutoCommit"] as Boolean,
            autoCommitIntervalMs = properties["autoCommitIntervalMs"] as Int,
            acks = properties["acks"] as String,
            batchSize = properties["batchSize"] as Int,
            lingerMs = properties["lingerMs"] as Int,
            compressionType = KafkaProducerConfig.CompressionType.valueOf(
                (properties["compressionType"] as String).uppercase()
            )
        )

        val encryptedConfig = KafkaEncryptedConfig(
            bootstrapServers = properties["bootstrapServers"] as String,
        )

        properties["schemaRegistryUrl"]?.let {
            encryptedConfig.apply {
                schemaRegistryUrl = it as String
            }

            brokerConfig.apply {
                enableSchemaRegistry = true
            }
        }

        return Pair(brokerConfig, encryptedConfig)
    }

    private fun parseRabbitConfiguration(
        properties: Map<String, Any?>
    ): Pair<RabbitProducerConfig, RabbitEncryptedConfig> {
        val brokerConfig = RabbitProducerConfig(
            allowAsync = properties["allowAsync"] as Boolean,
            retryMaxAttempts = properties["retryMaxAttempts"] as Int,
            retryBackoff = properties["retryBackoff"] as Int,
            connectionTimeout = properties["connectionTimeout"] as Int,
            errorHandlerStrategy = ProducerConfig.ErrorStrategy.valueOf(
                (properties["errorHandlerStrategy"] as String?)?.uppercase() ?: ProducerConfig.ErrorStrategy.DLQ.name
            ),
            exchangeName = properties["exchangeName"] as String?,
            queueName = properties["queueName"] as String?,
            prefetchCount = properties["prefetchCount"] as Int,
            publisherReturns = properties["publisherReturns"] as Boolean,
            sync = properties["sync"] as Boolean,
            channelCacheSize = properties["channelCacheSize"] as Int,
            defaultRoutingKey = properties["defaultRoutingKey"] as String?
        )

        val encryptedConfig = RabbitEncryptedConfig(
            host = properties["host"] as String,
            port = properties["port"] as Int,
            username = properties["username"] as String,
            password = properties["password"] as String,
            virtualHost = properties["virtualHost"] as String
        )

        return Pair(brokerConfig, encryptedConfig)
    }

    private fun parseSQSConfiguration(
        properties: Map<String, Any?>
    ): Pair<SQSProducerConfig, SQSEncryptedConfig> {
        val brokerConfig = SQSProducerConfig(
            allowAsync = properties["allowAsync"] as Boolean,
            retryMaxAttempts = properties["retryMaxAttempts"] as Int,
            retryBackoff = properties["retryBackoff"] as Int,
            connectionTimeout = properties["connectionTimeout"] as Int,
            errorHandlerStrategy = ProducerConfig.ErrorStrategy.valueOf(
                (properties["errorHandlerStrategy"] as String?)?.uppercase() ?: ProducerConfig.ErrorStrategy.DLQ.name
            ),
            queueUrl = properties["queueUrl"] as String,
            defaultGroupId = properties["defaultGroupId"] as String?,
            messageRetentionPeriod = properties["messageRetentionPeriod"] as Int,
            visibilityTimeout = properties["visibilityTimeout"] as Int,
            maxNumberOfMessages = properties["maxNumberOfMessages"] as Int
        )

        val encryptedConfig = SQSEncryptedConfig(
            accessKey = properties["accessKey"] as String,
            secretKey = properties["secretKey"] as String,
            region = properties["region"] as Region,
            endpointURL = properties["endpointURL"] as String
        )

        return Pair(brokerConfig, encryptedConfig)
    }
}