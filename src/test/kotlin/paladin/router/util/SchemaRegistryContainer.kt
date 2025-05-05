package paladin.router.util

import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.kafka.ConfluentKafkaContainer
import org.testcontainers.utility.DockerImageName

class SchemaRegistryContainer(image: DockerImageName) : GenericContainer<SchemaRegistryContainer>(image) {
    companion object {
        const val SCHEMA_REGISTRY_PORT = 8081
        const val SCHEMA_REGISTRY_NETWORK_ALIAS = "schema-registry"
    }

    init {
        withExposedPorts(SCHEMA_REGISTRY_PORT)
        withEnv("SCHEMA_REGISTRY_HOST_NAME", SCHEMA_REGISTRY_NETWORK_ALIAS)
        withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:$SCHEMA_REGISTRY_PORT")
    }

    fun registerKafkaContainer(kafkaContainer: ConfluentKafkaContainer): SchemaRegistryContainer {
        // Set network alias for schema registry container
        withNetwork(kafkaContainer.network)
        withNetworkAliases(SCHEMA_REGISTRY_NETWORK_ALIAS)

        // Connect to Kafka using the network alias
        withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9092")

        // Wait for Kafka to be ready before starting Schema Registry
        dependsOn(kafkaContainer)
        waitingFor(Wait.forHttp("/subjects").forStatusCode(200).forPort(SCHEMA_REGISTRY_PORT))

        return this
    }

    val schemaRegistryUrl: String
        get() = "http://${host}:${getMappedPort(SCHEMA_REGISTRY_PORT)}"
}