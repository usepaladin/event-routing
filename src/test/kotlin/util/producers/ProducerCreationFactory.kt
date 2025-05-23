package util.brokers

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory
import org.testcontainers.containers.RabbitMQContainer
import org.testcontainers.containers.localstack.LocalStackContainer
import paladin.router.enums.configuration.Broker
import paladin.router.models.configuration.producers.ProducerCreationRequest
import paladin.router.models.configuration.producers.auth.KafkaEncryptedConfig
import paladin.router.models.configuration.producers.auth.RabbitEncryptedConfig
import paladin.router.models.configuration.producers.auth.SQSEncryptedConfig
import paladin.router.models.configuration.producers.core.KafkaProducerConfig
import paladin.router.models.configuration.producers.core.RabbitProducerConfig
import paladin.router.models.configuration.producers.core.SQSProducerConfig
import paladin.router.util.fromDataclassToMap
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sqs.SqsClient
import util.KafkaCluster
import util.MessageBrokerCluster

object ProducerCreationFactory {

    fun fromKafka(
        name: String,
        cluster: KafkaCluster,
        keySerializationFormat: Broker.ProducerFormat,
        valueSerializationFormat: Broker.ProducerFormat,
        requireKey: Boolean = false,
        config: KafkaProducerConfig? = null,
    ): ProducerCreationRequest {

        val defaultKafkaProducerConfig = config.let {
            it ?: KafkaProducerConfig(
                clientId = name,
                groupId = "test_group_1",
                allowAsync = true,
                enableAutoCommit = false,
                autoCommitIntervalMs = 5000,
                requireKey = requireKey,
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
            producerName = name,
            brokerType = Broker.BrokerType.KAFKA,
            keySerializationFormat = keySerializationFormat,
            valueSerializationFormat = valueSerializationFormat,
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
        requireKey: Boolean = false,
        config: SQSProducerConfig? = null,
    ): ProducerCreationRequest {
        val defaultSqsProducerConfig = config ?: SQSProducerConfig(
            queueUrl = cluster.boostrapUrls.first(),
            defaultGroupId = "test_group_1",
            requireKey = requireKey,
        )

        val encryptedConfig = SQSEncryptedConfig(
            accessKey = cluster.container.accessKey,
            secretKey = cluster.container.secretKey,
            region = Region.of(cluster.container.region),
            endpointURL = cluster.container.endpoint.toString(),
        )

        return ProducerCreationRequest(
            producerName = name,
            brokerType = Broker.BrokerType.SQS,
            keySerializationFormat = null,
            valueSerializationFormat = valueSerializationFormat,
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
        requireKey: Boolean = false,
        queue: String,
        exchange: String = "paladin",
        config: RabbitProducerConfig? = null,
        routingKey: String = "#"
    ): ProducerCreationRequest {

        val defaultRabbitProducerConfig = config ?: RabbitProducerConfig(
            brokerType = Broker.BrokerType.RABBIT,
            exchangeName = exchange,
            queueName = queue,
            requireKey = requireKey,
            defaultRoutingKey = routingKey,
        )

        val encryptedConfig = RabbitEncryptedConfig(
            host = cluster.container.host,
            port = cluster.container.amqpPort,
            username = cluster.container.adminUsername,
            password = cluster.container.adminPassword,
        )

        return ProducerCreationRequest(
            producerName = name,
            brokerType = Broker.BrokerType.RABBIT,
            keySerializationFormat = null,
            valueSerializationFormat = valueSerializationFormat,
            configuration = fromDataclassToMap(
                listOf(
                    defaultRabbitProducerConfig,
                    encryptedConfig
                )
            )
        )
    }
}