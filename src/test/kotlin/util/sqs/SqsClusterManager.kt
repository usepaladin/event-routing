package util.sqs

import org.springframework.test.context.DynamicPropertyRegistry
import org.testcontainers.containers.localstack.LocalStackContainer
import org.testcontainers.utility.DockerImageName
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sqs.SqsClient
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest
import util.MessageBrokerCluster

class SqsClusterManager {

    private val clusters = mutableMapOf<String, MessageBrokerCluster<LocalStackContainer, SqsClient>>()

    fun init(id: String): MessageBrokerCluster<LocalStackContainer, SqsClient> {
        if (clusters.containsKey(id)) {
            throw IllegalStateException("SQS Cluster $id already initialized")
        }

        val container = LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8.1"))
            .withServices(LocalStackContainer.Service.SQS)
            .apply { start() }

        val sqsClient = SqsClient.builder()
            .endpointOverride(container.endpoint)
            .region(Region.US_EAST_1) // Explicitly set the region
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create("test", "test") // Set dummy credentials for LocalStack
                )
            )
            .build()

        return MessageBrokerCluster(container, sqsClient).also {
            clusters[id] = it
        }

    }

    // Register Spring properties for a specific cluster
    fun registerProperties(
        clusterId: String,
        registry: DynamicPropertyRegistry,
        propertyPrefix: String = "spring.cloud.aws.sqs.clusters.$clusterId"
    ) {
        val config = clusters[clusterId] ?: throw IllegalStateException("Cluster $clusterId not initialized")
        registry.add("$propertyPrefix.endpoint") { config.container.endpoint.toString() }
        registry.add("$propertyPrefix.credentials.access-key") { "test" }
        registry.add("$propertyPrefix.credentials.secret-key") { "test" }
        registry.add("$propertyPrefix.region.static") { "us-east-1" }

    }

    // Create a queue in the specified cluster
    fun createQueue(clusterId: String, queueName: String): String {
        val config = clusters[clusterId] ?: throw IllegalStateException("Cluster $clusterId not initialized")
        val createQueueRequest = CreateQueueRequest
            .builder()
            .queueName(queueName)
            .build()
        config.client.createQueue(createQueueRequest)
        val queueUrl = config.client.getQueueUrl { it.queueName(queueName) }.queueUrl()
        config.boostrapUrls.add(queueUrl)
        return queueUrl
    }

    // Get the SqsClient for a specific cluster
    fun getClient(clusterId: String): SqsClient {
        return clusters[clusterId]?.client ?: throw IllegalStateException("Cluster $clusterId not initialized")
    }

    // Clean up a specific cluster
    private fun cleanup(clusterId: String) {
        val config = clusters.remove(clusterId) ?: return
        config.boostrapUrls.forEach { url ->
            config.client.deleteQueue(DeleteQueueRequest.builder().queueUrl(url).build())
        }
        config.client.close()
        config.container.stop()
    }

    // Clean up all clusters
    fun cleanupAll() {
        clusters.keys.toList().forEach { cleanup(it) }
    }
}