package paladin.router.models.dispatch

import com.rabbitmq.client.ConnectionFactory
import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.apache.avro.generic.GenericRecord
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory
import org.springframework.amqp.rabbit.connection.CorrelationData
import org.springframework.amqp.rabbit.core.RabbitTemplate
import paladin.router.enums.configuration.Broker
import paladin.router.models.configuration.producers.MessageProducer
import paladin.router.models.configuration.producers.auth.RabbitEncryptedConfig
import paladin.router.models.configuration.producers.core.RabbitProducerConfig
import paladin.router.services.schema.SchemaService
import java.util.*

/**
 * A dispatcher for sending messages to a RabbitMQ broker using a [RabbitTemplate].
 * Supports STRING, JSON, and AVRO serialization formats with optional schema registry integration.
 *
 * @param producer The Rabbit instance used for setting up the message producer
 * @param producer The producer configuration (e.g., acks, retries).
 * @param connectionConfig Configuration properties for Broker connection/authentication (e.g., host, ports, credentials).
 * @param schemaService The service for serializing payloads.
 * @param meterRegistry Optional registry for metrics (e.g., Micrometer).
 */
data class RabbitDispatcher(
    override val producer: MessageProducer,
    override val producerConfig: RabbitProducerConfig,
    override val connectionConfig: RabbitEncryptedConfig,
    override val schemaService: SchemaService,
    override val meterRegistry: MeterRegistry? = null
) : MessageDispatcher() {

    private var messageProducer: RabbitTemplate? = null
    private val lock = Any()
    override val logger: KLogger = KotlinLogging.logger {}
    private val sendTimer: Timer? = meterRegistry?.timer("rabbitmq.dispatcher.send", "producer", name())
    private val errorCounter: Counter? =
        meterRegistry?.counter("rabbitmq.dispatcher.errors", "producer", name())


    /**
     * Dispatches a message to the specified RabbitMQ exchange or queue without a routing key.
     * Uses an empty routing key for the default exchange or a configured default key.
     *
     * @param payload The message payload.
     * @param topic The destination exchange/queue and serialization details.
     */
    override fun <V> dispatch(payload: V, topic: DispatchTopic) {
        synchronized(lock) {
            messageProducer.let {
                if (it == null) {
                    // Producer has not been instantiated
                    errorCounter?.increment()
                    throw IllegalStateException("Dispatcher is currently not built or connection has failed at the time of event production")
                }

                val dispatchValue =
                    schemaService.convertToFormat(payload, topic.value, topic.valueSchema).let transform@{ value ->
                        // Rabbit does not support Generic Record, convert to Byte Array
                        if (topic.value == Broker.ProducerFormat.AVRO) {
                            return@transform schemaService.avroToByteArray(value as GenericRecord)
                        }

                        value
                    }

                try {
                    val runnable = Runnable {
                        if (!producerConfig.allowAsync) {
                            val correlationData = CorrelationData(UUID.randomUUID().toString())
                            it.convertSendAndReceive(
                                topic.destinationTopic,
                                producerConfig.defaultRoutingKey ?: "",
                                dispatchValue,
                                correlationData
                            )
                            logger.info { "${identifier()} => Message sent synchronously to topic: $topic" }
                        } else {
                            messageProducer?.convertAndSend(
                                topic.destinationTopic,
                                producerConfig.defaultRoutingKey ?: "",
                                dispatchValue
                            )
                        }
                    }
                    sendTimer?.record(runnable) ?: runnable.run()
                } catch (ex: Exception) {
                    errorCounter?.increment()
                    throw ex
                }

            }
        }
    }

    /**
     * Dispatches a message to the specified RabbitMQ exchange or queue with a routing key.
     * Supports synchronous or asynchronous sending based on [RabbitProducerConfig.sync].
     *
     * @param key The routing key for the message.
     * @param payload The message payload.
     * @param topic The destination exchange/queue and serialization details.
     */
    override fun <K, V> dispatch(key: K, payload: V, topic: DispatchTopic) {
        synchronized(lock) {
            messageProducer.let {
                if (it == null) {
                    // Producer has not been instantiated
                    errorCounter?.increment()
                    throw IllegalStateException("Dispatcher is currently not built or connection has failed at the time of event production")
                }

                val dispatchKey =
                    schemaService.convertToFormat(payload, topic.key, topic.keySchema).let transform@{ key ->
                        // Rabbit does not support Generic Record, convert to Byte Array
                        if (topic.key == Broker.ProducerFormat.AVRO) {
                            return@transform schemaService.avroToByteArray(key as GenericRecord)
                        }

                        key
                    }

                val dispatchValue =
                    schemaService.convertToFormat(payload, topic.value, topic.valueSchema).let transform@{ value ->
                        // Rabbit does not support Generic Record, convert to Byte Array
                        if (topic.value == Broker.ProducerFormat.AVRO) {
                            return@transform schemaService.avroToByteArray(value as GenericRecord)
                        }

                        value
                    }

                try {
                    val runnable = Runnable {
                        if (!producerConfig.allowAsync) {
                            val correlationData = CorrelationData(UUID.randomUUID().toString())
                            it.convertSendAndReceive(
                                topic.destinationTopic,
                                dispatchKey.toString(),
                                dispatchValue,
                                correlationData
                            )
                            logger.info { "${identifier()} => Message sent synchronously to topic: $topic" }
                        } else {
                            messageProducer?.convertAndSend(
                                topic.destinationTopic,
                                dispatchKey.toString(),
                                dispatchValue
                            )
                        }
                    }
                    sendTimer?.record(runnable) ?: runnable.run()
                } catch (ex: Exception) {
                    errorCounter?.increment()
                    throw ex
                }

            }
        }
    }

    override fun build() {
        synchronized(lock) {
            this.updateConnectionState(MessageDispatcherState.Building)

            // Create ConnectionFactory
            val connectionFactory = ConnectionFactory().apply {
                host = connectionConfig.host
                port = connectionConfig.port
                virtualHost = connectionConfig.virtualHost
                if (connectionConfig.sslEnabled) {
                    useSslProtocol()
                }
            }

            connectionConfig.username?.let { connectionFactory.username = it }
            connectionConfig.password?.let { connectionFactory.password = it }
            connectionConfig.virtualHost.let { connectionFactory.virtualHost = it }


            // Create CachingConnectionFactory for Spring
            val cachingConnectionFactory = CachingConnectionFactory(connectionFactory).apply {
                isPublisherReturns = producerConfig.publisherReturns
                channelCacheSize = producerConfig.channelCacheSize
                setConnectionTimeout(producerConfig.connectionTimeout.toInt())
            }

            // Create RabbitTemplate
            messageProducer = RabbitTemplate(cachingConnectionFactory).apply {
                setMandatory(producerConfig.publisherReturns) // Required for publisher returns
                setConfirmCallback { _, ack, cause ->
                    if (!ack) {
                        logger.error { "${identifier()} => Message delivery failed: $cause" }
                        errorCounter?.increment()
                    }
                }
                setReturnsCallback { returned ->
                    logger.error {
                        "${identifier()} => Message returned: ${returned.replyText}, exchange: ${returned.exchange}, routingKey: ${returned.routingKey}"
                    }
                    errorCounter?.increment()
                }
            }

            testConnection()
        }
    }

    override fun testConnection(): Boolean {
        synchronized(lock) {
            try {
                messageProducer?.connectionFactory?.createConnection()?.createChannel(
                    // Transactional/sync (true) v publish confirm/async (false)
                    !producerConfig.allowAsync
                )?.use { channel ->
                    // Test by declaring a temporary queue
                    channel.queueDeclarePassive(producerConfig.queueName) // Assumes brokerName is a valid queue for testing
                }
                logger.info { "${identifier()} => Connection successful" }
                this.updateConnectionState(MessageDispatcherState.Connected)
                return true
            } catch (e: Exception) {
                logger.error(e) { "${identifier()} => Connection failed" }
                this.updateConnectionState(MessageDispatcherState.Error(e))
                errorCounter?.increment()
                return false
            }
        }
    }

    /**
     * Closes the RabbitMQ producer by shutting down the [RabbitTemplate] and its connection factory.
     */
    override fun close() {
        synchronized(lock) {
            try {
                // Close the RabbitTemplate
                messageProducer?.stop()
                logger.info { "${identifier()} => Producer closed successfully" }
                this.updateConnectionState(MessageDispatcherState.Disconnected)
            } catch (e: Exception) {
                logger.error(e) { "${identifier()} => Error closing producer" }
                errorCounter?.increment()
            } finally {
                messageProducer = null
            }
        }
    }


    /**
     * Validates the configuration properties for the RabbitMQ Producer.
     */
    override fun validate() {
        if (connectionConfig.host.isEmpty()) {
            throw IllegalArgumentException("${identifier()} => Host cannot be null or empty")
        }
        if (connectionConfig.port <= 0) {
            throw IllegalArgumentException("${identifier()} => Port must be greater than 0")
        }
        if (connectionConfig.username.isNullOrEmpty()) {
            throw IllegalArgumentException("${identifier()} => Username cannot be null or empty")
        }
        if (connectionConfig.password.isNullOrEmpty()) {
            throw IllegalArgumentException("${identifier()} => Password cannot be null or empty")
        }
        if (producerConfig.retryMaxAttempts < 0) {
            throw IllegalArgumentException("${identifier()} => Retries cannot be less than 0")
        }
        if (producerConfig.retryBackoff <= 0) {
            throw IllegalArgumentException("${identifier()} => Retry backoff must be greater than 0")
        }
        if (producerConfig.connectionTimeout <= 0) {
            throw IllegalArgumentException("${identifier()} => Connection timeout must be greater than 0")
        }
        if (producerConfig.channelCacheSize <= 0) {
            throw IllegalArgumentException("${identifier()} => Channel cache size must be greater than 0")
        }
    }
}