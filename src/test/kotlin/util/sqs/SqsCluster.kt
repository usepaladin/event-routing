package util.sqs

import org.testcontainers.containers.localstack.LocalStackContainer
import software.amazon.awssdk.services.sqs.SqsClient

data class SqsCluster(
    val container: LocalStackContainer,
    val client: SqsClient,
    val boostrapUrls: MutableList<String>,
)