package util.kafka

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.springframework.test.context.DynamicPropertyRegistry
import org.testcontainers.containers.Network
import org.testcontainers.kafka.ConfluentKafkaContainer
import org.testcontainers.utility.DockerImageName
import util.KafkaCluster
import java.util.*

class KafkaClusterManager {
    // Store cluster configurations
    private val clusters = mutableMapOf<String, KafkaCluster>()

    // Initialize a new Kafka cluster with Schema Registry
    fun init(id: String, includeSchemaRegistry: Boolean = true): KafkaCluster {
        if (clusters.containsKey(id)) {
            throw IllegalStateException("Kafka Cluster $id already initialized")
        }

        // Create a network for Kafka and Schema Registry
        val network = Network.newNetwork()

        // Start Kafka container
        val kafkaContainer = ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"))
            .withListener("kafka:19092")
            .withNetwork(network)
            .withReuse(true)
            .apply { start() }

        val schemaRegistry: Pair<SchemaRegistryContainer, CachedSchemaRegistryClient?>? = includeSchemaRegistry.let {
            if (!it) return@let null

            val container = SchemaRegistryContainer(DockerImageName.parse("confluentinc/cp-schema-registry:7.4.0"))
                .withReuse(true)
                .apply {
                    withKafka(kafkaContainer)
                    start()
                }

            val client = CachedSchemaRegistryClient(container.schemaRegistryUrl, 100)
            container to client
        }

        // Create AdminClient for topic management
        val adminProps = Properties().apply {
            put("bootstrap.servers", kafkaContainer.bootstrapServers)
        }

        val adminClient = AdminClient.create(adminProps)

        return KafkaCluster(
            network = network,
            schemaRegistryContainer = schemaRegistry?.first,
            schemaRegistryClient = schemaRegistry?.second,
            container = kafkaContainer,
            client = adminClient,
            topics = mutableListOf()
        ).also {
            clusters[id] = it
        }
    }

    // Register Spring properties for a specific cluster
    fun registerProperties(
        id: String,
        registry: DynamicPropertyRegistry,
        propertyPrefix: String = "spring.kafka.clusters.$id"
    ) {
        val config = clusters[id] ?: throw IllegalStateException("Kafka Cluster $id not initialized")
        registry.add("$propertyPrefix.bootstrap-servers") { config.container.bootstrapServers }
        config.schemaRegistryContainer?.let {
            registry.add("$propertyPrefix.schema-registry-url") {
                it.schemaRegistryUrl
            }
        } ?: run {
            registry.add("$propertyPrefix.schema-registry-url") { null }
        }
    }

    // Create a topic in the specified cluster
    fun createTopic(id: String, topicName: String, partitions: Int = 1, replicationFactor: Short = 1) {
        val config = clusters[id] ?: throw IllegalStateException("Kafka Cluster $id not initialized")
        val newTopic = NewTopic(topicName, partitions, replicationFactor)
        config.run {
            client.createTopics(listOf(newTopic)).all().get()
            topics.add(newTopic)
        }
    }

    fun getCluster(id: String): KafkaCluster {
        return clusters[id] ?: throw IllegalStateException("Kafka Cluster $id not initialized")
    }

    // Clean up a specific cluster
    private fun cleanup(id: String) {
        val config = clusters.remove(id) ?: return
        config.client.close()
        config.schemaRegistryContainer?.stop()
        config.container.stop()
        config.network.close()
    }

    // Clean up all clusters
    fun cleanupAll() {
        clusters.keys.toList().forEach { cleanup(it) }
    }
}