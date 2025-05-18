package paladin.router.models.dispatch

import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import paladin.router.enums.configuration.Broker
import paladin.router.models.configuration.producers.MessageProducer
import paladin.router.models.configuration.producers.auth.KafkaEncryptedConfig
import paladin.router.models.configuration.producers.core.KafkaProducerConfig
import paladin.router.services.schema.SchemaService
import paladin.router.util.factory.SerializerFactory
import java.util.*

/**
 * A dispatcher for sending messages to a Kafka broker using a [KafkaProducer].
 * Supports STRING, JSON, and AVRO serialization formats with optional schema registry integration.
 *
 * @param producer The Kafka producer instance used for setting up the message producer
 * @param producer The producer configuration (e.g., acks, retries).
 * @param connectionConfig Configuration properties for Broker connection/authentication (e.g., bootstrap servers, schema registry URL).
 * @param schemaService The service for serializing payloads.
 * @param meterRegistry Optional registry for metrics (e.g., Micrometer).
 */
data class KafkaDispatcher(
    override val producer: MessageProducer,
    override val producerConfig: KafkaProducerConfig,
    override val connectionConfig: KafkaEncryptedConfig,
    override val schemaService: SchemaService,
    override val meterRegistry: MeterRegistry? = null,
) : MessageDispatcher() {

    private var client: KafkaProducer<Any, Any>? = null
    private val lock = Any()
    private val sendTimer: Timer? = meterRegistry?.timer(
        "kafka.dispatcher.send",
        "producer", name()
    )

    private val errorCounter: Counter? = meterRegistry?.counter(
        "kafka.dispatcher.error",
        "producer", name()
    )

    override val logger: KLogger = KotlinLogging.logger {}

    override fun <V> dispatch(payload: V, topic: DispatchTopic) {
        synchronized(lock) {
            client.let {
                if (it == null) {
                    // Producer has not been instantiated
                    errorCounter?.increment()
                    throw IllegalStateException("Dispatcher is currently not built or connection has failed at the time of event production")
                }

                val dispatchValue = schemaService.convertToFormat(payload, topic.value, topic.valueSchema)
                val record: ProducerRecord<Any, Any> = ProducerRecord(topic.destinationTopic, dispatchValue)
                try {
                    val runnable = produceMessage(it, topic.destinationTopic, record)
                    // Use the timer to measure the time taken for the send operation, else fire the runnable directly
                    sendTimer?.record(runnable) ?: runnable.run()
                } catch (e: Exception) {
                    errorCounter?.increment()
                    throw e
                }
            }
        }
    }

    override fun <K, V> dispatch(key: K, payload: V, topic: DispatchTopic) {
        synchronized(lock) {
            client.let {
                if (it == null) {
                    // Producer has not been instantiated
                    errorCounter?.increment()
                    throw IllegalStateException("Dispatcher is currently not built or connected at the time of event production")
                }

                val dispatchKey = schemaService.convertToFormat(key, topic.key, topic.keySchema)
                val dispatchValue = schemaService.convertToFormat(payload, topic.value, topic.valueSchema)
                val record: ProducerRecord<Any, Any> =
                    ProducerRecord(topic.destinationTopic, dispatchKey, dispatchValue)
                try {
                    val runnable = produceMessage(it, topic.destinationTopic, record)
                    // Use the timer to measure the time taken for the send operation, else fire the runnable directly
                    sendTimer?.record(runnable) ?: runnable.run()
                } catch (e: Exception) {
                    errorCounter?.increment()
                    throw e
                }
            }
        }
    }

    private fun produceMessage(
        client: KafkaProducer<Any, Any>,
        topic: String,
        record: ProducerRecord<Any, Any>
    ): Runnable {
        return Runnable {
            if (!producerConfig.allowAsync) {
                client.send(record).get()
                logger.info { "${identifier()} => Message sent synchronously to topic: $topic" }
            } else {
                client.send(record) { metadata, exception ->
                    if (exception != null) {
                        errorCounter?.increment()
                        logger.error(exception) {
                            "${identifier()} => Error sending message to topic: $topic"
                        }
                    } else {
                        logger.info {
                            "${identifier()} => Message sent asynchronously to topic: $topic, " +
                                    "partition: ${metadata.partition()}, offset: ${metadata.offset()}"
                        }
                    }
                }
            }
        }
    }

    override fun build() {
        synchronized(lock) {
            this.updateConnectionState(MessageDispatcherState.Building)
            val properties = Properties()
            properties.apply {
                put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, connectionConfig.bootstrapServers)
                put(
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, SerializerFactory.fromFormat(
                        producer.keySerializationFormat,
                        requiresSchemaRegistry
                    )
                )
                put(
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    SerializerFactory.fromFormat(producer.valueSerializationFormat, requiresSchemaRegistry)
                )
                put(ProducerConfig.ACKS_CONFIG, producerConfig.acks)
                put(ProducerConfig.RETRIES_CONFIG, producerConfig.retryMaxAttempts)
                put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, producerConfig.retryBackoff)
                put(ProducerConfig.BATCH_SIZE_CONFIG, producerConfig.batchSize)
                put(ProducerConfig.LINGER_MS_CONFIG, producerConfig.lingerMs)
                put(ProducerConfig.COMPRESSION_TYPE_CONFIG, producerConfig.compressionType.type)
            }

            properties.apply {
                if (requiresSchemaRegistry) {
                    put("schema.registry.url", connectionConfig.schemaRegistryUrl)
                    put("specific.avro.reader", true)
                    return@apply
                }

                put("specific.avro.reader", false)
            }

            client = KafkaProducer(properties)
            testConnection()
        }
    }

    override fun testConnection() {
        try {
            val adminProps = Properties().apply {
                put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, connectionConfig.bootstrapServers)
            }
            AdminClient.create(adminProps).use { adminClient ->
                adminClient.listTopics().names().get()
                logger.info { "${identifier()} => Connection successful" }
                this.updateConnectionState(MessageDispatcherState.Connected)
            }
        } catch (e: Exception) {
            logger.error(e) { "${identifier()} => Connection failed" }
            this.updateConnectionState(MessageDispatcherState.Error(e))
        }
    }

    override fun validate() {
        if (connectionConfig.bootstrapServers.isNullOrEmpty()) {
            throw IllegalArgumentException("${identifier()} => Bootstrap servers cannot be null or empty")
        }
        if (producerConfig.acks.isEmpty()) {
            throw IllegalArgumentException("${identifier()} => Acks cannot be null or empty")
        }
        if (producerConfig.retryMaxAttempts < 0) {
            throw IllegalArgumentException("${identifier()} => Retries cannot be less than 0")
        }
        if (producerConfig.retryBackoff <= 0) {
            throw IllegalArgumentException("${identifier()} => Request timeout must be greater than 0")
        }

        if (this.requiresSchemaRegistry && connectionConfig.schemaRegistryUrl.isNullOrEmpty()) {
            throw IllegalArgumentException("${identifier()} => Schema registry URL cannot be null or empty for Avro format")
        }
    }

    override fun close() {
        synchronized(lock) {
            try {
                client?.flush()
                client?.close()
                logger.info { "${identifier()} => Producer closed successfully" }
                this.updateConnectionState(MessageDispatcherState.Disconnected)
            } catch (e: Exception) {
                logger.error(e) { "${identifier()} => Error closing producer" }
            } finally {
                client = null
            }
        }
    }

    /**
     * Assert requirement to use Schema Registry for Avro serialisation
     * Or if a Schema Registry URL is provided (Ie. For JSON Schemas)
     */
    private val requiresSchemaRegistry: Boolean
        get() = producerConfig.enableSchemaRegistry &&
                (producer.valueSerializationFormat == Broker.ProducerFormat.AVRO ||
                        producer.keySerializationFormat == Broker.ProducerFormat.AVRO ||
                        producer.valueSerializationFormat == Broker.ProducerFormat.JSON ||
                        producer.keySerializationFormat == Broker.ProducerFormat.JSON)
}