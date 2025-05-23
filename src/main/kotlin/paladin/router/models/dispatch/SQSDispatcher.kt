package paladin.router.models.dispatch

import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import paladin.router.models.configuration.producers.MessageProducer
import paladin.router.models.configuration.producers.auth.SQSEncryptedConfig
import paladin.router.models.configuration.producers.core.SQSProducerConfig
import paladin.router.services.schema.SchemaService
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.services.sqs.SqsClient
import software.amazon.awssdk.services.sqs.model.SendMessageRequest
import java.net.URI

/**
 * A dispatcher for sending messages to an Amazon SQS queue using [SqsClient].
 * Supports STRING, JSON, and AVRO serialization formats with optional schema validation.
 *
 * @param broker The SQS broker configuration.
 * @param config The producer configuration (e.g., retries, sync/async).
 * @param connectionConfig The authentication configuration (e.g., access key, secret key, endpoint).
 * @param schemaService The service for serializing payloads.
 * @param meterRegistry Optional registry for metrics (e.g., Micrometer).
 */
data class SqsDispatcher(
    override val producer: MessageProducer,
    override val producerConfig: SQSProducerConfig,
    override val connectionConfig: SQSEncryptedConfig,
    override val schemaService: SchemaService,
    override val meterRegistry: MeterRegistry? = null
) : MessageDispatcher(), AutoCloseable {

    private var client: SqsClient? = null
    private val lock = Any()
    override val logger: KLogger = KotlinLogging.logger {}
    private val sendTimer: Timer? = meterRegistry?.timer("sqs.dispatcher.send", "broker", name())
    private val errorCounter: Counter? = meterRegistry?.counter("sqs.dispatcher.errors", "broker", name())

    /**
     * Dispatches a message to the specified SQS queue with a message group ID.
     * Supports synchronous or asynchronous sending based on [SQSProducerConfig.allowAsync].
     *
     * @param key The message group ID (for FIFO queues) or deduplication ID.
     * @param payload The message payload.
     * @param topic The destination queue URL and serialization details.
     */
    override fun <K, V> dispatch(key: K, payload: V, topic: DispatchTopic) {
        synchronized(lock) {
            client.let {
                if (it == null) {
                    errorCounter?.increment()
                    throw IllegalStateException("Dispatcher is currently not built or connection has failed at the time of event production")
                }

                val dispatchKey = schemaService.convertToFormat(key, topic.key, topic.keySchema)
                val dispatchValue = schemaService.convertToFormat(payload, topic.value, topic.valueSchema)

                val requestBuilder: SendMessageRequest.Builder = SendMessageRequest.builder()
                    .queueUrl(topic.destinationTopic)
                    .messageBody(dispatchValue.toString())

                if (producerConfig.fifoQueue) {
                    requestBuilder.apply {
                        // For FIFO queues, set the message group ID and deduplication ID
                        messageGroupId(dispatchKey.toString()) // For FIFO queues
                        messageDeduplicationId(dispatchKey.toString()) // For FIFO deduplication
                    }
                }

                val runnable = sendMessage(requestBuilder.build(), it, topic.destinationTopic)
                sendTimer?.record(runnable) ?: runnable.run()

            }
        }
    }

    /**
     * Dispatches a message to the specified SQS queue without a message group ID.
     * Uses the default group ID from [SQSProducerConfig.defaultGroupId] or none for standard queues.
     *
     * @param payload The message payload.
     * @param topic The destination queue URL and serialization details.
     */
    override fun <V> dispatch(payload: V, topic: DispatchTopic) {
        synchronized(lock) {
            client.let {
                if (it == null) {
                    errorCounter?.increment()
                    throw IllegalStateException("Dispatcher is currently not built or connection has failed at the time of event production")
                }

                val dispatchValue = schemaService.convertToFormat(payload, topic.value, topic.valueSchema)
                // Use the default group ID if provided, otherwise no group ID for standard queues
                val requestBuilder: SendMessageRequest.Builder = SendMessageRequest.builder()
                    .queueUrl(topic.destinationTopic)
                    .messageBody(dispatchValue.toString())

                if (producerConfig.fifoQueue) {
                    requestBuilder.apply {
                        producerConfig.defaultGroupId?.let { id ->
                            messageGroupId(id).messageDeduplicationId(
                                id
                            )
                        }
                    }
                }

                val runnable = sendMessage(requestBuilder.build(), it, topic.destinationTopic)
                sendTimer?.record(runnable) ?: runnable.run()
            }
        }
    }

    private fun sendMessage(request: SendMessageRequest, client: SqsClient, topic: String): Runnable {
        return Runnable {
            try {
                client.sendMessage(request)
                logger.info { "SQS Broker => Broker name: ${name()} => Message sent to queue: $topic" }
            } catch (e: Exception) {
                logger.error(e) { "SQS Broker => Broker name: ${name()} => Error sending message to queue: $topic" }
                errorCounter?.increment()
            }
        }
    }

    /**
     * Builds the SQS client by initializing the [SqsClient] with the provided configuration.
     */
    override fun build() {
        synchronized(lock) {
            if (client != null) {
                logger.warn { "SQS Broker => Broker name: ${name()} => Client already initialized, skipping build" }
                return
            }

            this.updateConnectionState(MessageDispatcherState.Building)

            try {
                val credentials =
                    AwsBasicCredentials.create(connectionConfig.accessKey, connectionConfig.secretKey)
                client = SqsClient.builder()
                    .apply {
                        connectionConfig.endpointURL?.let { endpointOverride(URI.create(it)) }
                    }
                    .region(connectionConfig.region)
                    .credentialsProvider(StaticCredentialsProvider.create(credentials))
                    .build()

                this.producerConfig.fifoQueue.let {
                    if (it) {
                        client?.setQueueAttributes { client ->
                            client.queueUrl(this.producerConfig.queueUrl)
                                .attributesWithStrings(mapOf("FifoQueue" to "true"))
                        }

                    }
                }

                testConnection()
            } catch (e: Exception) {
                logger.error(e) { "SQS Broker => Broker name: ${name()} => Failed to build client" }
                this.updateConnectionState(MessageDispatcherState.Error(e))
            }
        }
    }

    /**
     * Tests the connection to the SQS service by listing queues.
     */
    override fun testConnection(): Boolean {
        synchronized(lock) {
            try {
                client?.listQueues()
                logger.info { "SQS Broker => Broker name: ${name()} => Connection successful" }
                this.updateConnectionState(MessageDispatcherState.Connected)
                return true
            } catch (e: Exception) {
                logger.error(e) { "SQS Broker => Broker name: ${name()} => Connection failed" }
                errorCounter?.increment()
                this.updateConnectionState(MessageDispatcherState.Error(e))
                return false
            }
        }
    }

    /**
     * Validates the configuration properties for the SQS broker.
     */
    override fun validate() {
        if (connectionConfig.accessKey.isEmpty()) {
            throw IllegalArgumentException("SQS Broker => Broker name: ${name()} => Access key ID cannot be null or empty")
        }
        if (connectionConfig.secretKey.isEmpty()) {
            throw IllegalArgumentException("SQS Broker => Broker name: ${name()} => Secret access key cannot be null or empty")
        }
        if (producerConfig.retryMaxAttempts < 0) {
            throw IllegalArgumentException("SQS Broker => Broker name: ${name()} => Retries cannot be less than 0")
        }
        if (producerConfig.retryBackoff <= 0) {
            throw IllegalArgumentException("SQS Broker => Broker name: ${name()} => Retry backoff must be greater than 0")
        }
    }

    /**
     * Closes the SQS client.
     */
    override fun close() {
        synchronized(lock) {
            try {
                client?.close()
                logger.info { "SQS Broker => Broker name: ${name()} => Client closed successfully" }
                this.updateConnectionState(MessageDispatcherState.Disconnected)
            } catch (e: Exception) {
                logger.error(e) { "SQS Broker => Broker name: ${name()} => Error closing client" }
                errorCounter?.increment()
            } finally {
                client = null
            }
        }
    }

}