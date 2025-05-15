package util.rabbit

import com.rabbitmq.client.ConnectionFactory
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory
import org.springframework.test.context.DynamicPropertyRegistry
import org.testcontainers.containers.RabbitMQContainer
import org.testcontainers.utility.DockerImageName
import util.MessageBrokerCluster

class RabbitClusterManager {
    // Store cluster configurations
    private val clusters = mutableMapOf<String, MessageBrokerCluster<RabbitMQContainer, CachingConnectionFactory>>()

    // Initialize a new RabbitMQ cluster
    fun init(clusterId: String): MessageBrokerCluster<RabbitMQContainer, CachingConnectionFactory> {
        if (clusters.containsKey(clusterId)) {
            throw IllegalStateException("RabbitMQ Cluster $clusterId already initialized")
        }

        val container = RabbitMQContainer(DockerImageName.parse("rabbitmq:3.13-management"))
            .withExposedPorts(5672, 15672) // AMQP and Management API ports
            .apply { start() }

        val factory = ConnectionFactory().apply {
            host = container.host
            port = container.getMappedPort(5672)
            username = container.adminUsername
            password = container.adminPassword
        }
        val connectionFactory = CachingConnectionFactory(factory)

        val config = MessageBrokerCluster(container, connectionFactory)
        clusters[clusterId] = config
        return config
    }

    // Register Spring properties for a specific cluster
    fun registerProperties(
        clusterId: String,
        registry: DynamicPropertyRegistry,
        propertyPrefix: String = "spring.rabbitmq.clusters.$clusterId"
    ) {
        val config = clusters[clusterId] ?: throw IllegalStateException("RabbitMQ Cluster $clusterId not initialized")
        registry.add("$propertyPrefix.host") { config.container.host }
        registry.add("$propertyPrefix.port") { config.container.getMappedPort(5672).toString() }
        registry.add("$propertyPrefix.username") { config.container.adminUsername }
        registry.add("$propertyPrefix.password") { config.container.adminPassword }
    }

    // Create a queue in the specified cluster
    fun createQueue(clusterId: String, queueName: String): String {
        val config = clusters[clusterId] ?: throw IllegalStateException("RabbitMQ Cluster $clusterId not initialized")
        config.client.createConnection().createChannel(true).use { channel ->
            channel.queueDeclare(queueName, true, false, false, null)
            config.boostrapUrls.add(queueName)
            return queueName
        }
    }

    // Get the ConnectionFactory for a specific cluster
    fun getConnectionFactory(clusterId: String): CachingConnectionFactory {
        return clusters[clusterId]?.client
            ?: throw IllegalStateException("RabbitMQ Cluster $clusterId not initialized")
    }

    // Clean up a specific cluster
    private fun cleanup(clusterId: String) {
        val config = clusters.remove(clusterId) ?: return
        config.client.destroy()
        config.container.stop()
    }

    // Clean up all clusters
    fun cleanupAll() {
        clusters.keys.toList().forEach { cleanup(it) }
    }
}