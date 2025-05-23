package util

import com.rabbitmq.client.Channel
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory
import org.testcontainers.containers.Network
import org.testcontainers.containers.RabbitMQContainer
import org.testcontainers.kafka.ConfluentKafkaContainer
import util.kafka.SchemaRegistryContainer

open class MessageBrokerCluster<T, Q>(
    open val container: T,
    open val client: Q,
    val boostrapUrls: MutableList<String> = mutableListOf(),
)

data class KafkaCluster(
    val network: Network,
    val schemaRegistryContainer: SchemaRegistryContainer? = null,
    val schemaRegistryClient: SchemaRegistryClient? = null,
    val topics: MutableList<NewTopic> = mutableListOf(),
    override val container: ConfluentKafkaContainer,
    override val client: AdminClient,
) : MessageBrokerCluster<ConfluentKafkaContainer, AdminClient>(container, client)

data class RabbitMqCluster(
    val channel: Channel,
    override val container: RabbitMQContainer,
    override val client: CachingConnectionFactory

) : MessageBrokerCluster<RabbitMQContainer, CachingConnectionFactory>(
    container, client
)