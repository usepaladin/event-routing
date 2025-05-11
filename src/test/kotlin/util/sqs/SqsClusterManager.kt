package util.sqs

import org.springframework.test.context.DynamicPropertyRegistry
import software.amazon.awssdk.services.sqs.SqsClient
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest

class SqsClusterManager {

    private val clusters = mutableMapOf<String, SqsCluster>()

    fun initCluster(id: String): SqsCluster {}

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
        registry.add("$propertyPrefix.region.static") { config.container.region }

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
    fun cleanup(clusterId: String) {
        val config = clusters.remove(clusterId) ?: return
        config.boostrapUrls.forEach { url ->
            config.client.deleteQueue(DeleteQueueRequest.builder().queueUrl(url).build())
        }
        config.client.close()
        config.container.stop()
    }

    // Clean up all clusters
    fun cleanupAll() {
        clusters.keys.toList().forEach { cleanupCluster(it) }
    }
}