package util.brokers

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory
import org.testcontainers.containers.RabbitMQContainer
import org.testcontainers.containers.localstack.LocalStackContainer
import paladin.router.enums.configuration.Broker
import paladin.router.enums.configuration.sqs.Region
import paladin.router.models.configuration.brokers.ProducerCreationRequest
import paladin.router.models.configuration.brokers.auth.KafkaEncryptedConfig
import paladin.router.models.configuration.brokers.auth.RabbitEncryptedConfig
import paladin.router.models.configuration.brokers.auth.SQSEncryptedConfig
import paladin.router.models.configuration.brokers.core.KafkaProducerConfig
import paladin.router.models.configuration.brokers.core.RabbitProducerConfig
import paladin.router.models.configuration.brokers.core.SQSProducerConfig
import paladin.router.util.fromDataclassToMap
import software.amazon.awssdk.services.sqs.SqsClient
import util.KafkaCluster
import util.MessageBrokerCluster

object ProducerCreationFactory {

    fun fromKafka(
        name: String,
        cluster: KafkaCluster,
        keySerializationFormat: Broker.ProducerFormat,
        valueSerializationFormat: Broker.ProducerFormat,
        config: KafkaProducerConfig? = null,
    ): ProducerCreationRequest {

        val defaultKafkaProducerConfig = config.let {
            it ?: KafkaProducerConfig(
                clientId = name,
                groupId = null,
                enableAutoCommit = false,
                autoCommitIntervalMs = 5000,
                requestTimeoutMs = 30000,
                retries = 5,
                acks = "all"
            )
        }

        val encryptedProducerConfig = KafkaEncryptedConfig(
            bootstrapServers = cluster.container.bootstrapServers,
        )

        cluster.schemaRegistryContainer?.let {
            encryptedProducerConfig.schemaRegistryUrl = it.schemaRegistryUrl
        }

        return ProducerCreationRequest(
            brokerName = name,
            brokerType = Broker.BrokerType.KAFKA,
            keySerializationFormat = keySerializationFormat,
            valueSerializationFormat = valueSerializationFormat,
            defaultBroker = false,
            configuration = fromDataclassToMap(
                listOf(
                    defaultKafkaProducerConfig,
                    encryptedProducerConfig
                )
            )
        )
    }

    fun fromSqs(
        name: String,
        cluster: MessageBrokerCluster<LocalStackContainer, SqsClient>,
        valueSerializationFormat: Broker.ProducerFormat,
        config: SQSProducerConfig? = null,
    ): ProducerCreationRequest {
        val defaultSqsProducerConfig = config ?: SQSProducerConfig(
            brokerType = Broker.BrokerType.SQS,
            queueUrl = cluster.boostrapUrls.first(),
        )

        val encryptedConfig = SQSEncryptedConfig(
            accessKey = cluster.container.accessKey,
            secretKey = cluster.container.secretKey,
            region = Region.fromRegion(cluster.container.region) ?: Region.US_EAST_1,
        )

        return ProducerCreationRequest(
            brokerName = name,
            brokerType = Broker.BrokerType.SQS,
            keySerializationFormat = null,
            valueSerializationFormat = valueSerializationFormat,
            defaultBroker = false,
            configuration = fromDataclassToMap(
                listOf(
                    defaultSqsProducerConfig,
                    encryptedConfig
                )
            )
        )
    }

    fun fromRabbit(
        name: String,
        cluster: MessageBrokerCluster<RabbitMQContainer, CachingConnectionFactory>,
        valueSerializationFormat: Broker.ProducerFormat,
        config: RabbitProducerConfig? = null,
        queue: String
    ): ProducerCreationRequest {

        val defaultRabbitProducerConfig = config ?: RabbitProducerConfig(
            brokerType = Broker.BrokerType.RABBIT,
            exchangeName = "paladin",
            queueName = queue
        )

        val encryptedConfig = RabbitEncryptedConfig(
            host = cluster.container.host,
            port = cluster.container.amqpPort,
            username = cluster.container.adminUsername,
            password = cluster.container.adminPassword,
        )

        return ProducerCreationRequest(
            brokerName = name,
            brokerType = Broker.BrokerType.RABBIT,
            keySerializationFormat = null,
            valueSerializationFormat = valueSerializationFormat,
            defaultBroker = false,
            configuration = fromDataclassToMap(
                listOf(
                    defaultRabbitProducerConfig,
                    encryptedConfig
                )
            )
        )
    }
}