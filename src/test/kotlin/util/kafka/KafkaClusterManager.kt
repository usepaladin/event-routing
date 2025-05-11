package util.kafka

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.apache.kafka.clients.admin.AdminClient
import org.testcontainers.containers.Network
import org.testcontainers.kafka.ConfluentKafkaContainer

class KafkaTestClusterManager {
    // Store cluster configurations
    private val clusters = mutableMapOf<String, ClusterConfig>()

    data class ClusterConfig(
        val network: Network,
        val kafkaContainer: ConfluentKafkaContainer,
        val schemaRegistryContainer: SchemaRegistryContainer,
        val adminClient: AdminClient,
        val schemaRegistryClient: SchemaRegistryClient,
        val topics: MutableList<String> = mutableListOf()
    )

    // Initialize a new Kafka cluster with Schema Registry
    fun initializeCluster(clusterId: String): ClusterConfig {
        if (clusters.containsKey(clusterId)) {
            throw IllegalStateException("Kafka Cluster $clusterId already initialized")
        }

        // Create a network for Kafka and Schema Registry
        val network = Network.newNetwork()

        // Start Kafka container
        val kafkaContainer = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"))
            .withNetwork(network)
            .withNetworkAliases("kafka")
            .apply { start() }

        // Start Schema Registry container
        val schemaRegistryContainer = GenericContainer(DockerImageName.parse("confluentinc/cp-schema-registry:7.5.0"))
            .withNetwork(network)
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "kafka:9092")
            .withExposedPorts(8081)
            .apply { start() }

        // Create AdminClient for topic management
        val adminProps = Properties().apply {
            put("bootstrap.servers", kafkaContainer.bootstrapServers)
        }
        val adminClient = AdminClient.create(adminProps)

        // Create SchemaRegistryClient
        val schemaRegistryUrl = "http://${schemaRegistryContainer.host}:${schemaRegistryContainer.getMappedPort(8081)}"
        val schemaRegistryClient = CachedSchemaRegistryClient(schemaRegistryUrl, 100)

        val config = ClusterConfig(network, kafkaContainer, schemaRegistryContainer, adminClient, schemaRegistryClient)
        clusters[clusterId] = config
        return config
    }

    // Register Spring properties for a specific cluster
    fun registerProperties(
        clusterId: String,
        registry: DynamicPropertyRegistry,
        propertyPrefix: String = "spring.kafka.clusters.$clusterId"
    ) {
        val config = clusters[clusterId] ?: throw IllegalStateException("Kafka Cluster $clusterId not initialized")
        registry.add("$propertyPrefix.bootstrap-servers") { config.kafkaContainer.bootstrapServers }
        registry.add("$propertyPrefix.schema-registry-url") {
            "http://${config.schemaRegistryContainer.host}:${config.schemaRegistryContainer.getMappedPort(8081)}"
        }
    }

    // Create a topic in the specified cluster
    fun createTopic(clusterId: String, topicName: String, partitions: Int = 1, replicationFactor: Short = 1) {
        val config = clusters[clusterId] ?: throw IllegalStateException("Kafka Cluster $clusterId not initialized")
        val newTopic = NewTopic(topicName, partitions, replicationFactor)
        config.adminClient.createTopics(listOf(newTopic)).all().get()
        config.topics.add(topicName)
    }

    // Get the AdminClient for a specific cluster
    fun getAdminClient(clusterId: String): AdminClient {
        return clusters[clusterId]?.adminClient
            ?: throw IllegalStateException("Kafka Cluster $clusterId not initialized")
    }

    // Get the SchemaRegistryClient for a specific cluster
    fun getSchemaRegistryClient(clusterId: String): SchemaRegistryClient {
        return clusters[clusterId]?.schemaRegistryClient
            ?: throw IllegalStateException("Kafka Cluster $clusterId not initialized")
    }

    // Clean up a specific cluster
    fun cleanupCluster(clusterId: String) {
        val config = clusters.remove(clusterId) ?: return
        config.adminClient.close()
        config.schemaRegistryContainer.stop()
        config.kafkaContainer.stop()
        config.network.close()
    }

    // Clean up all clusters
    fun cleanupAll() {
        clusters.keys.toList().forEach { cleanupCluster(it) }
    }
}