package paladin.router.models.dispatch

import com.rabbitmq.client.ConnectionFactory
import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory
import org.springframework.amqp.rabbit.core.RabbitTemplate
import paladin.router.models.configuration.brokers.MessageProducer
import paladin.router.models.configuration.brokers.auth.RabbitEncryptedConfig
import paladin.router.models.configuration.brokers.core.RabbitProducerConfig
import paladin.router.services.schema.SchemaService

data class RabbitDispatcher(
    override val broker: MessageProducer,
    override val config: RabbitProducerConfig,
    override val authConfig: RabbitEncryptedConfig,
    override val schemaService: SchemaService
) : MessageDispatcher() {

    private var producer: RabbitTemplate? = null

    override val logger: KLogger
        get() = KotlinLogging.logger { }

    override fun <V> dispatch(payload: V, topic: DispatchTopic) {
        TODO("Not yet implemented")
    }

    override fun <K, V> dispatch(key: K, payload: V, topic: DispatchTopic) {
        if (producer == null) {
            logger.error { "RabbitMQ Broker => Broker name: ${broker.brokerName} => Unable to send message => Producer has not been instantiated" }
            return
        }

        val dispatchKey = schemaService.convertToFormat(key, topic.key, topic.keySchema)
        val dispatchValue = schemaService.convertToFormat(payload, topic.value, topic.valueSchema)

        try {
            // Send message to the exchange with the routing key (or queue if exchange is empty)
            producer?.convertAndSend(
                topic.destinationTopic, // Exchange name
                dispatchKey.toString(),  // Routing key
                dispatchValue
            )
            logger.info { "RabbitMQ Broker => Broker name: ${broker.brokerName} => Message sent successfully to topic: $topic" }
        } catch (e: Exception) {
            logger.error(e) { "RabbitMQ Broker => Broker name: ${broker.brokerName} => Error sending message to topic: $topic" }
            this.updateConnectionState(MessageDispatcherState.Error(e))
        }
    }

    override fun testConnection() {
        try {
            producer?.connectionFactory?.createConnection()?.createChannel(true)?.use { channel ->
                // Test by declaring a temporary queue
                channel.queueDeclarePassive(broker.brokerName) // Assumes brokerName is a valid queue for testing
            }
            logger.info { "RabbitMQ Broker => Broker name: ${broker.brokerName} => Connection successful" }
            this.updateConnectionState(MessageDispatcherState.Connected)
        } catch (e: Exception) {
            logger.error(e) { "RabbitMQ Broker => Broker name: ${broker.brokerName} => Connection failed" }
            this.updateConnectionState(MessageDispatcherState.Error(e))
        }
    }

    override fun build() {
        this.updateConnectionState(MessageDispatcherState.Building)

        // Create ConnectionFactory
        val connectionFactory = ConnectionFactory().apply {
            host = authConfig.host
            port = authConfig.port
            virtualHost = authConfig.virtualHost
        }

        authConfig.username?.let { connectionFactory.username = it }
        authConfig.password?.let { connectionFactory.password = it }
        authConfig.virtualHost.let { connectionFactory.virtualHost = it }


        // Create CachingConnectionFactory for Spring
        val cachingConnectionFactory = CachingConnectionFactory(connectionFactory).apply {
            isPublisherReturns = config.publisherReturns
        }

        // Create RabbitTemplate
        producer = RabbitTemplate(cachingConnectionFactory).apply {
            setMandatory(config.publisherReturns) // Required for publisher returns
            setConfirmCallback { _, ack, cause ->
                if (!ack) {
                    logger.error { "RabbitMQ Broker => Broker name: ${broker.brokerName} => Message delivery failed: $cause" }
                }
            }
            setReturnsCallback { returned ->
                logger.error {
                    "RabbitMQ Broker => Broker name: ${broker.brokerName} => Message returned: ${returned.replyText}, exchange: ${returned.exchange}, routingKey: ${returned.routingKey}"
                }
            }
        }

        testConnection()
    }

    override fun validate() {
        if (authConfig.host.isEmpty()) {
            throw IllegalArgumentException("RabbitMQ Broker => Broker name: ${broker.brokerName} => Host cannot be null or empty")
        }
        if (authConfig.port <= 0) {
            throw IllegalArgumentException("RabbitMQ Broker => Broker name: ${broker.brokerName} => Port must be greater than 0")
        }
        if (authConfig.username.isNullOrEmpty()) {
            throw IllegalArgumentException("RabbitMQ Broker => Broker name: ${broker.brokerName} => Username cannot be null or empty")
        }
        if (authConfig.password.isNullOrEmpty()) {
            throw IllegalArgumentException("RabbitMQ Broker => Broker name: ${broker.brokerName} => Password cannot be null or empty")
        }
    }
}