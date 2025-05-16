package paladin.router.models.dispatch

import com.rabbitmq.client.ConnectionFactory
import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory
import org.springframework.amqp.rabbit.connection.CorrelationData
import org.springframework.amqp.rabbit.core.RabbitTemplate
import paladin.router.models.configuration.brokers.MessageProducer
import paladin.router.models.configuration.brokers.auth.RabbitEncryptedConfig
import paladin.router.models.configuration.brokers.core.RabbitProducerConfig
import paladin.router.services.schema.SchemaService
import java.util.*

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
    private val sendTimer: Timer? = meterRegistry?.timer("rabbitmq.dispatcher.send", "producer", producer.producerName)
    private val errorCounter: Counter? =
        meterRegistry?.counter("rabbitmq.dispatcher.errors", "producer", producer.producerName)


    /**
     * Dispatches a message to the specified RabbitMQ exchange or queue without a routing key.
     * Uses an empty routing key for the default exchange or a configured default key.
     *
     * @param payload The message payload.
     * @param topic The destination exchange/queue and serialization details.
     */
    override fun <V> dispatch(payload: V, topic: DispatchTopic) {
        dispatch(producerConfig.defaultRoutingKey ?: "", payload, topic)
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
                    logger.error { "RabbitMQ Producer => Producer name: ${name()} => Unable to send message => Producer has not been instantiated" }
                    return
                }


                val dispatchKey = schemaService.convertToFormat(key, topic.key, topic.keySchema)
                val dispatchValue = schemaService.convertToFormat(payload, topic.value, topic.valueSchema)
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
                            logger.info { "RabbitMQ Producer => Producer name: ${name()} => Message sent synchronously to topic: $topic" }
                        } else {
                            messageProducer?.convertAndSend(
                                topic.destinationTopic,
                                dispatchKey.toString(),
                                dispatchValue
                            )
                        }
                    }
                } catch (ex: Exception) {
                    errorCounter?.increment()
                    logger.error(ex) { "RabbitMQ Producer => Producer name: ${name()} => Error sending message to topic: $topic" }
                    this.updateConnectionState(MessageDispatcherState.Error(ex))
                }

            }
        }
    }


    override fun testConnection() {
        synchronized(lock) {
            try {
                messageProducer?.connectionFactory?.createConnection()?.createChannel(true)?.use { channel ->
                    // Test by declaring a temporary queue
                    channel.queueDeclarePassive(name()) // Assumes brokerName is a valid queue for testing
                }
                logger.info { "RabbitMQ Producer => Producer name: ${name()} => Connection successful" }
                this.updateConnectionState(MessageDispatcherState.Connected)
            } catch (e: Exception) {
                logger.error(e) { "RabbitMQ Producer => Producer name: ${name()} => Connection failed" }
                this.updateConnectionState(MessageDispatcherState.Error(e))
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
                        logger.error { "RabbitMQ Producer => Producer name: ${name()} => Message delivery failed: $cause" }
                        errorCounter?.increment()
                    }
                }
                setReturnsCallback { returned ->
                    logger.error {
                        "RabbitMQ Producer => Producer name: ${name()} => Message returned: ${returned.replyText}, exchange: ${returned.exchange}, routingKey: ${returned.routingKey}"
                    }
                    errorCounter?.increment()
                }
            }

            testConnection()
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
                logger.info { "RabbitMQ Producer => Producer name: ${name()} => Producer closed successfully" }
                this.updateConnectionState(MessageDispatcherState.Disconnected)
            } catch (e: Exception) {
                logger.error(e) { "RabbitMQ Producer => Producer name: ${name()} => Error closing producer" }
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
            throw IllegalArgumentException("RabbitMQ Producer => Producer name: ${name()} => Host cannot be null or empty")
        }
        if (connectionConfig.port <= 0) {
            throw IllegalArgumentException("RabbitMQ Producer => Producer name: ${name()} => Port must be greater than 0")
        }
        if (connectionConfig.username.isNullOrEmpty()) {
            throw IllegalArgumentException("RabbitMQ Producer => Producer name: ${name()} => Username cannot be null or empty")
        }
        if (connectionConfig.password.isNullOrEmpty()) {
            throw IllegalArgumentException("RabbitMQ Producer => Producer name: ${name()} => Password cannot be null or empty")
        }
        if (producerConfig.retryMaxAttempts < 0) {
            throw IllegalArgumentException("RabbitMQ Producer => Producer name: ${name()} => Retries cannot be less than 0")
        }
        if (producerConfig.retryBackoff <= 0) {
            throw IllegalArgumentException("RabbitMQ Producer => Producer name: ${name()} => Retry backoff must be greater than 0")
        }
        if (producerConfig.connectionTimeout <= 0) {
            throw IllegalArgumentException("RabbitMQ Producer => Producer name: ${name()} => Connection timeout must be greater than 0")
        }
        if (producerConfig.channelCacheSize <= 0) {
            throw IllegalArgumentException("RabbitMQ Producer => Producer name: ${name()} => Channel cache size must be greater than 0")
        }
    }

    private fun name(): String {
        return producer.producerName
    }
}