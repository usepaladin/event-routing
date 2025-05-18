package paladin.router.models.dispatch

import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import paladin.router.models.configuration.brokers.MessageProducer
import paladin.router.models.configuration.brokers.auth.SQSEncryptedConfig
import paladin.router.models.configuration.brokers.core.SQSProducerConfig
import paladin.router.services.schema.SchemaService
import software.amazon.awssdk.services.sqs.SqsClient
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
                    val errorMsg =
                        "SQS Broker => Broker name: ${name()} => Unable to send message => Client has not been instantiated"
                    logger.error { errorMsg }
                    errorCounter?.increment()
                    if (config.throwOnError) throw IllegalStateException(errorMsg)
                    return
                }
            }
            if (client == null) {
                val errorMsg =
                    "SQS Broker => Broker name: ${name()} => Unable to send message => Client has not been instantiated"
                logger.error { errorMsg }
                errorCounter?.increment()
                if (config.throwOnError) throw IllegalStateException(errorMsg)
                return
            }

            val dispatchKey = convertToFormat(key, topic.key, topic.keySchema)
            val dispatchValue = convertToFormat(payload, topic.value, topic.valueSchema)

            val request = SendMessageRequest.builder()
                .queueUrl(topic.destinationTopic)
                .messageBody(dispatchValue.toString())
                .messageGroupId(dispatchKey.toString()) // For FIFO queues
                .messageDeduplicationId(dispatchKey.toString()) // For FIFO deduplication
                .build()

            var lastException: Exception? = null
            repeat(config.retries + 1) { attempt ->
                sendTimer?.record {
                    try {
                        if (config.sync) {
                            client?.sendMessage(request)
                            logger.info {
                                "SQS Broker => Broker name: ${name()} => Message sent synchronously to queue: $topic, groupId: $dispatchKey"
                            }
                            return@repeat
                        } else {
                            client?.sendMessage(request) // SqsClient is sync; use SqsAsyncClient for true async
                            logger.info {
                                "SQS Broker => Broker name: ${name()} => Message sent asynchronously to queue: $topic, groupId: $dispatchKey"
                            }
                            return@repeat
                        }
                    } catch (e: Exception) {
                        lastException = e
                        if (attempt < config.retries) {
                            logger.warn {
                                "SQS Broker => Broker name: ${name()} => Retry attempt ${attempt + 1} for queue: $topic, groupId: $dispatchKey"
                            }
                            Thread.sleep(config.retryBackoffMs)
                        }
                    }
                }
            }

            if (lastException != null) {
                val errorMsg =
                    "SQS Broker => Broker name: ${name()} => Error sending message to queue: $topic, groupId: $dispatchKey"
                logger.error(lastException) { errorMsg }
                errorCounter?.increment()
                this.updateConnectionState(MessageDispatcherState.Error(lastException))
                if (config.throwOnError) throw lastException
            }
        }
    }

    /**
     * Dispatches a message to the specified SQS queue without a message group ID.
     * Uses the default group ID from [SqsProducerConfig.defaultGroupId] or none for standard queues.
     *
     * @param payload The message payload.
     * @param topic The destination queue URL and serialization details.
     */
    override fun <V> dispatch(payload: V, topic: DispatchTopic) {
        synchronized(lock) {
            if (client == null) {
                val errorMsg =
                    "SQS Broker => Broker name: ${name()} => Unable to send message => Client has not been instantiated"
                logger.error { errorMsg }
                errorCounter?.increment()
                if (config.throwOnError) throw IllegalStateException(errorMsg)
                return
            }

            val dispatchValue = convertToFormat(payload, topic.value, topic.valueSchema)

            val request = SendMessageRequest.builder()
                .queueUrl(topic.destinationTopic)
                .messageBody(dispatchValue.toString())
                .apply { config.defaultGroupId?.let { messageGroupId(it).messageDeduplicationId(it) } }
                .build()

            var lastException: Exception? = null
            repeat(config.retries + 1) { attempt ->
                sendTimer?.record {
                    try {
                        if (config.sync) {
                            client?.sendMessage(request)
                            logger.info {
                                "SQS Broker => Broker name: ${name()} => Message sent synchronously to queue: $topic (no key)"
                            }
                            return@repeat
                        } else {
                            client?.sendMessage(request)
                            logger.info {
                                "SQS Broker => Broker name: ${name()} => Message sent asynchronously to queue: $topic (no key)"
                            }
                            return@repeat
                        }
                    } catch (e: Exception) {
                        lastException = e
                        if (attempt < config.retries) {
                            logger.warn {
                                "SQS Broker => Broker name: ${name()} => Retry attempt ${attempt + 1} for queue: $topic (no key)"
                            }
                            Thread.sleep(config.retryBackoffMs)
                        }
                    }
                }
            }

            if (lastException != null) {
                val errorMsg =
                    "SQS Broker => Broker name: ${name()} => Error sending message to queue: $topic (no key)"
                logger.error(lastException) { errorMsg }
                errorCounter?.increment()
                this.updateConnectionState(MessageDispatcherState.Error(lastException))
                if (config.throwOnError) throw lastException
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
                    AwsBasicCredentials.create(connectionConfig.accessKeyId, connectionConfig.secretAccessKey)
                client = SqsClient.builder()
                    .region(Region.of(connectionConfig.region))
                    .credentialsProvider(StaticCredentialsProvider.create(credentials))
                    .apply { connectionConfig.endpoint?.let { endpointOverride(URI.create(it)) } }
                    .build()

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
    override fun testConnection() {
        synchronized(lock) {
            try {
                client?.listQueues()
                logger.info { "SQS Broker => Broker name: ${name()} => Connection successful" }
                this.updateConnectionState(MessageDispatcherState.Connected)
            } catch (e: Exception) {
                logger.error(e) { "SQS Broker => Broker name: ${name()} => Connection failed" }
                errorCounter?.increment()
                this.updateConnectionState(MessageDispatcherState.Error(e))
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
        if (config.retries < 0) {
            throw IllegalArgumentException("SQS Broker => Broker name: ${name()} => Retries cannot be less than 0")
        }
        if (config.retryBackoffMs <= 0) {
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

    private fun name(): String {
        return producer.producerName
    }
}