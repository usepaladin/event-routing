package util.kafka

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
        waitingFor(Wait.forHttp("/subjects").forStatusCode(200))
        withExposedPorts(SCHEMA_REGISTRY_PORT)
    }

    fun withKafka(kafkaContainer: ConfluentKafkaContainer): SchemaRegistryContainer {
        // Set network alias for schema registry container
        return this
            .withNetwork(kafkaContainer.network)
            // Connect to Kafka using the network alias
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", SCHEMA_REGISTRY_NETWORK_ALIAS)
            .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:$SCHEMA_REGISTRY_PORT")
            .withEnv(
                "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
                "PLAINTEXT://kafka:19092"
            )
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_SECURITY_PROTOCOL", "PLAINTEXT")

            // Wait for Kafka to be ready before starting Schema Registry
            .dependsOn(kafkaContainer)
            .waitingFor(Wait.forHttp("/subjects").forStatusCode(200).forPort(SCHEMA_REGISTRY_PORT))
    }

    val schemaRegistryUrl: String
        get() = "http://${host}:${getMappedPort(SCHEMA_REGISTRY_PORT)}"
}