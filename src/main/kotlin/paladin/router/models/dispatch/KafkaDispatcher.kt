package paladin.router.models.dispatch

import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import paladin.router.enums.configuration.Broker
import paladin.router.models.configuration.brokers.MessageProducer
import paladin.router.models.configuration.brokers.auth.KafkaEncryptedConfig
import paladin.router.models.configuration.brokers.core.KafkaProducerConfig
import paladin.router.services.schema.SchemaService
import paladin.router.util.factory.SerializerFactory
import java.util.*


data class KafkaDispatcher(
    override val producerConfig: MessageProducer,
    override val brokerConfig: KafkaProducerConfig,
    override val brokerAuthConfig: KafkaEncryptedConfig,
    override val schemaService: SchemaService,
    override val meterRegistry: MeterRegistry? = null,
) : MessageDispatcher() {

    private var producer: KafkaProducer<Any, Any>? = null
    private val lock = Any()
    private val sendTimer: Timer? = meterRegistry?.timer(
        "kafka.dispatcher.send",
        "broker", name()
    )
    override val logger: KLogger = KotlinLogging.logger {}

    override fun <V> dispatch(payload: V, topic: DispatchTopic) {
        TODO("Not yet implemented")
    }

    override fun <K, V> dispatch(key: K, payload: V, topic: DispatchTopic) {
        synchronized(lock) {
            producer.let {
                if (it == null) {
                    logger.error { "Kafka Broker => Broker name: ${name()} => Unable to send message => Producer has not been instantiated" }
                    return
                }

                val dispatchKey = schemaService.convertToFormat(key, topic.key, topic.keySchema)
                val dispatchValue = schemaService.convertToFormat(payload, topic.value, topic.valueSchema)
                val record: ProducerRecord<Any, Any> =
                    ProducerRecord(topic.destinationTopic, dispatchKey, dispatchValue)

                try {
                    if (!brokerConfig.allowAsync) {
                        // Send synchronously
                        producer?.send(record)?.get()
                        logger.info { "Kafka Broker => Broker name: ${name()} => Message sent synchronously to topic: $topic" }
                    } else {
                        // Send asynchronously and handle the callback at a later time
                        producer?.send(record) { metadata, exception ->
                            if (exception != null) {
                                logger.error(exception) {
                                    "Kafka Broker => Broker name: ${name()} => Error sending message to topic: $topic"
                                }
                            } else {
                                logger.info {
                                    "Kafka Broker => Broker name: ${name()} => Message sent asynchronously to topic: $topic, " +
                                            "partition: ${metadata.partition()}, offset: ${metadata.offset()}"
                                }
                            }
                        }
                    }
                } catch (e: Exception) {
                    logger.error(e) { "Kafka Broker => Broker name: ${name()} => Error sending message to topic: $topic" }
                    this.updateConnectionState(MessageDispatcherState.Error(e))
                }
            }
        }
    }

    override fun build() {
        synchronized(lock) {
            this.updateConnectionState(MessageDispatcherState.Building)
            val properties = Properties()
            properties.apply {
                put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAuthConfig.bootstrapServers)
                put(
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, SerializerFactory.fromFormat(
                        producerConfig.keySerializationFormat,
                        requiresSchemaRegistry
                    )
                )
                put(
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    SerializerFactory.fromFormat(producerConfig.valueSerializationFormat, requiresSchemaRegistry)
                )
                put(ProducerConfig.ACKS_CONFIG, brokerConfig.acks)
                put(ProducerConfig.RETRIES_CONFIG, brokerConfig.retries)
                put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, brokerConfig.requestTimeoutMs)
                put(ProducerConfig.BATCH_SIZE_CONFIG, brokerConfig.batchSize)
                put(ProducerConfig.LINGER_MS_CONFIG, brokerConfig.lingerMs)
                put(ProducerConfig.COMPRESSION_TYPE_CONFIG, brokerConfig.compressionType.type)
            }

            properties.apply {
                if (requiresSchemaRegistry) {
                    put("schema.registry.url", brokerAuthConfig.schemaRegistryUrl)
                    put("specific.avro.reader", true)
                    return@apply
                }

                put("specific.avro.reader", false)
            }

            producer = KafkaProducer(properties)
            testConnection()
        }
    }

    override fun testConnection() {
        try {
            val adminProps = Properties().apply {
                put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAuthConfig.bootstrapServers)
            }
            AdminClient.create(adminProps).use { adminClient ->
                adminClient.listTopics().names().get()
                logger.info { "Kafka Broker => Broker name: ${name()} => Connection successful" }
                this.updateConnectionState(MessageDispatcherState.Connected)
            }
        } catch (e: Exception) {
            logger.error(e) { "Kafka Broker => Broker name: ${name()} => Connection failed" }
            this.updateConnectionState(MessageDispatcherState.Error(e))
        }
    }

    override fun validate() {
        if (brokerAuthConfig.bootstrapServers.isNullOrEmpty()) {
            throw IllegalArgumentException("Kafka Broker => Broker name: ${name()} => Bootstrap servers cannot be null or empty")
        }
        if (brokerConfig.acks.isEmpty()) {
            throw IllegalArgumentException("Kafka Broker => Broker name: ${name()} => Acks cannot be null or empty")
        }
        if (brokerConfig.retries < 0) {
            throw IllegalArgumentException("Kafka Broker => Broker name: ${name()} => Retries cannot be less than 0")
        }
        if (brokerConfig.requestTimeoutMs <= 0) {
            throw IllegalArgumentException("Kafka Broker => Broker name: ${name()} => Request timeout must be greater than 0")
        }

        if (this.requiresSchemaRegistry && brokerAuthConfig.schemaRegistryUrl.isNullOrEmpty()) {
            throw IllegalArgumentException("Kafka Broker => Broker name: ${name()} => Schema registry URL cannot be null or empty for Avro format")
        }
    }

    override fun close() {
        synchronized(lock) {
            try {
                producer?.flush()
                producer?.close()
                logger.info { "Kafka Broker => Broker name: ${name()} => Producer closed successfully" }
                this.updateConnectionState(MessageDispatcherState.Disconnected)
            } catch (e: Exception) {
                logger.error(e) { "Kafka Broker => Broker name: ${name()} => Error closing producer" }
            } finally {
                producer = null
            }
        }
    }

    private fun name(): String {
        return producerConfig.producerName
    }

    /**
     * Assert requirement to use Schema Registry for Avro serialisation
     * Or if a Schema Registry URL is provided (Ie. For JSON Schemas)
     */
    private val requiresSchemaRegistry: Boolean
        get() = brokerConfig.enableSchemaRegistry &&
                (producerConfig.valueSerializationFormat == Broker.ProducerFormat.AVRO ||
                        producerConfig.keySerializationFormat == Broker.ProducerFormat.AVRO ||
                        producerConfig.valueSerializationFormat == Broker.ProducerFormat.JSON ||
                        producerConfig.keySerializationFormat == Broker.ProducerFormat.JSON)
}