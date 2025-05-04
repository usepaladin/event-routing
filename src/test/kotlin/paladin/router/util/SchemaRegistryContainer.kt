package paladin.router.util

import org.testcontainers.containers.GenericContainer
import org.testcontainers.kafka.ConfluentKafkaContainer
import org.testcontainers.utility.DockerImageName

class SchemaRegistryContainer(image: DockerImageName) : GenericContainer<SchemaRegistryContainer>(image) {
    init {
        withExposedPorts(8081)
        withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
        withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
    }

    fun withKafka(kafkaContainer: ConfluentKafkaContainer): SchemaRegistryContainer {
        withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", kafkaContainer.bootstrapServers)
        dependsOn(kafkaContainer)
        return this
    }

    val schemaRegistryUrl: String
        get() = "http://${host}:${getMappedPort(8081)}"
}