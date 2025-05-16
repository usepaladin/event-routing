package paladin.router.models.dispatch

import com.rabbitmq.client.ConnectionFactory
import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory
import org.springframework.amqp.rabbit.core.RabbitTemplate
import paladin.router.models.configuration.brokers.MessageProducer
import paladin.router.models.configuration.brokers.auth.RabbitEncryptedConfig
import paladin.router.models.configuration.brokers.core.RabbitProducerConfig
import paladin.router.services.schema.SchemaService

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
        synchronized(lock) {
            if (messageProducer == null) {
                val errorMsg =
                    "RabbitMQ Broker => Broker name: ${name()} => Unable to send message => Producer has not been instantiated"
                logger.error { errorMsg }
                errorCounter?.increment()
                return
            }

            val dispatchValue = schemaService.convertToFormat(payload, topic.value, topic.valueSchema)
            val routingKey = producerConfig.defaultRoutingKey ?: "" // Use default or empty routing key

            var lastException: Exception? = null
            repeat(producerConfig.retryMaxAttempts + 1) { attempt ->
                sendTimer?.record {
                    try {
                        if (config.sync) {
                            messageProducer.convertAndSend(topic.destinationTopic, routingKey, dispatchValue)
                            logger.info {
                                "RabbitMQ Broker => Broker name: ${broker.brokerName} => Message sent synchronously to topic: $topic (no key)"
                            }
                            return@repeat
                        } else {
                            producer.convertAndSend(topic.destinationTopic, routingKey, dispatchValue)
                            logger.info {
                                "RabbitMQ Broker => Broker name: ${broker.brokerName} => Message sent asynchronously to topic: $topic (no key)"
                            }
                            return@repeat
                        }
                    } catch (e: Exception) {
                        lastException = e
                        if (attempt < config.retries) {
                            logger.warn {
                                "RabbitMQ Broker => Broker name: ${broker.brokerName} => Retry attempt ${attempt + 1} for topic: $topic (no key)"
                            }
                            Thread.sleep(config.retryBackoffMs)
                        }
                    }
                }
            }

            if (lastException != null) {
                val errorMsg =
                    "RabbitMQ Broker => Broker name: ${broker.brokerName} => Error sending message to topic: $topic (no key)"
                logger.error(lastException) { errorMsg }
                errorCounter?.increment()
                this.updateConnectionState(MessageDispatcherState.Error(lastException))
                if (config.throwOnError) throw lastException
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
            if (producer == null) {
                val errorMsg =
                    "RabbitMQ Broker => Broker name: ${broker.brokerName} => Unable to send message => Producer has not been instantiated"
                logger.error { errorMsg }
                errorCounter?.increment()
                if (config.throwOnError) throw IllegalStateException(errorMsg)
                return
            }

            val dispatchKey = convertToFormat(key, topic.key, topic.keySchema)
            val dispatchValue = convertToFormat(payload, topic.value, topic.valueSchema)

            var lastException: Exception? = null
            repeat(config.retries + 1) { attempt ->
                sendTimer?.record {
                    try {
                        if (config.sync) {
                            producer.convertAndSend(topic.destinationTopic, dispatchKey.toString(), dispatchValue)
                            logger.info {
                                "RabbitMQ Broker => Broker name: ${broker.brokerName} => Message sent synchronously to topic: $topic"
                            }
                            return@repeat
                        } else {
                            producer.convertAndSend(topic.destinationTopic, dispatchKey.toString(), dispatchValue)
                            logger.info {
                                "RabbitMQ Broker => Broker name: ${broker.brokerName} => Message sent asynchronously to topic: $topic"
                            }
                            return@repeat
                        }
                    } catch (e: Exception) {
                        lastException = e
                        if (attempt < config.retries) {
                            logger.warn {
                                "RabbitMQ Broker => Broker name: ${broker.brokerName} => Retry attempt ${attempt + 1} for topic: $topic"
                            }
                            Thread.sleep(config.retryBackoffMs)
                        }
                    }
                }
            }

            if (lastException != null) {
                val errorMsg =
                    "RabbitMQ Broker => Broker name: ${broker.brokerName} => Error sending message to topic: $topic"
                logger.error(lastException) { errorMsg }
                errorCounter?.increment()
                this.updateConnectionState(MessageDispatcherState.Error(lastException))
                if (config.throwOnError) throw lastException
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
                logger.info { "RabbitMQ Broker => Broker name: ${name()} => Connection successful" }
                this.updateConnectionState(MessageDispatcherState.Connected)
            } catch (e: Exception) {
                logger.error(e) { "RabbitMQ Broker => Broker name: ${name()} => Connection failed" }
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
                        logger.error { "RabbitMQ Broker => Broker name: ${name()} => Message delivery failed: $cause" }
                        errorCounter?.increment()
                    }
                }
                setReturnsCallback { returned ->
                    logger.error {
                        "RabbitMQ Broker => Broker name: ${name()} => Message returned: ${returned.replyText}, exchange: ${returned.exchange}, routingKey: ${returned.routingKey}"
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
                logger.info { "RabbitMQ Broker => Broker name: ${name()} => Producer closed successfully" }
                this.updateConnectionState(MessageDispatcherState.Disconnected)
            } catch (e: Exception) {
                logger.error(e) { "RabbitMQ Broker => Broker name: ${name()} => Error closing producer" }
                errorCounter?.increment()
            } finally {
                messageProducer = null
            }
        }
    }


    /**
     * Validates the configuration properties for the RabbitMQ broker.
     */
    override fun validate() {
        if (connectionConfig.host.isNullOrEmpty()) {
            throw IllegalArgumentException("RabbitMQ Broker => Broker name: ${name()} => Host cannot be null or empty")
        }
        if (connectionConfig.port <= 0) {
            throw IllegalArgumentException("RabbitMQ Broker => Broker name: ${name()} => Port must be greater than 0")
        }
        if (connectionConfig.username.isNullOrEmpty()) {
            throw IllegalArgumentException("RabbitMQ Broker => Broker name: ${name()} => Username cannot be null or empty")
        }
        if (connectionConfig.password.isNullOrEmpty()) {
            throw IllegalArgumentException("RabbitMQ Broker => Broker name: ${name()} => Password cannot be null or empty")
        }
        if (producerConfig.retryMaxAttempts < 0) {
            throw IllegalArgumentException("RabbitMQ Broker => Broker name: ${name()} => Retries cannot be less than 0")
        }
        if (producerConfig.retryBackoff <= 0) {
            throw IllegalArgumentException("RabbitMQ Broker => Broker name: ${name()} => Retry backoff must be greater than 0")
        }
        if (producerConfig.connectionTimeout <= 0) {
            throw IllegalArgumentException("RabbitMQ Broker => Broker name: ${name()} => Connection timeout must be greater than 0")
        }
        if (producerConfig.channelCacheSize <= 0) {
            throw IllegalArgumentException("RabbitMQ Broker => Broker name: ${name()} => Channel cache size must be greater than 0")
        }
    }

    private fun name(): String {
        return producer.producerName
    }
}