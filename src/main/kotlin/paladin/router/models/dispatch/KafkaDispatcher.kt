package paladin.router.models.dispatch

import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging
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
    override val broker: MessageProducer,
    override val config: KafkaProducerConfig,
    override val authConfig: KafkaEncryptedConfig,
    override val schemaService: SchemaService
) : MessageDispatcher() {
    private var producer: KafkaProducer<Any, Any>? = null
    override val logger: KLogger = KotlinLogging.logger {}

    override fun <V> dispatch(payload: V, topic: DispatchTopic) {
        TODO("Not yet implemented")
    }

    override fun <K, V> dispatch(key: K, payload: V, topic: DispatchTopic) {
        producer.let {
            if (it == null) {
                logger.error { "Kafka Broker => Broker name: ${broker.brokerName} => Unable to send message => Producer has not been instantiated" }
                return
            }

            val dispatchKey = schemaService.convertToFormat(key, topic.key, topic.keySchema)
            val dispatchValue = schemaService.convertToFormat(payload, topic.value, topic.valueSchema)
            val record: ProducerRecord<Any, Any> = ProducerRecord(topic.destinationTopic, dispatchKey, dispatchValue)

            try {
                it.send(record)?.get()
                logger.info { "Kafka Broker => Broker name: ${broker.brokerName} => Message sent successfully to topic: $topic" }
            } catch (e: Exception) {
                logger.error(e) { "Kafka Broker => Broker name: ${broker.brokerName} => Error sending message to topic: $topic" }
            }
        }
    }

    override fun build() {
        this.updateConnectionState(MessageDispatcherState.Building)
        val properties = Properties()
        properties.apply {
            put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, authConfig.bootstrapServers)
            put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, SerializerFactory.fromFormat(
                    broker.keySerializationFormat,
                    requiresSchemaRegistry
                )
            )
            put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                SerializerFactory.fromFormat(broker.valueSerializationFormat, requiresSchemaRegistry)
            )
            put(ProducerConfig.ACKS_CONFIG, config.acks)
            put(ProducerConfig.RETRIES_CONFIG, config.retries)
            put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, config.requestTimeoutMs)
        }

        properties.apply {
            if (requiresSchemaRegistry) {
                put("schema.registry.url", authConfig.schemaRegistryUrl)
                put("specific.avro.reader", true)
                return@apply
            }

            put("specific.avro.reader", false)
        }

        producer = KafkaProducer(properties)
        testConnection()
    }

    override fun testConnection() {
        try {
            producer?.partitionsFor(broker.brokerName)
            logger.info { "Kafka Broker => Broker name: ${broker.brokerName} => Connection successful" }
            this.updateConnectionState(MessageDispatcherState.Connected)
        } catch (e: Exception) {
            logger.error(e) { "Kafka Broker => Broker name: ${broker.brokerName} => Connection failed" }
            this.updateConnectionState(MessageDispatcherState.Error(e))
        }
    }

    override fun validate() {
        if (authConfig.bootstrapServers.isNullOrEmpty()) {
            throw IllegalArgumentException("Kafka Broker => Broker name: ${broker.brokerName} => Bootstrap servers cannot be null or empty")
        }
        if (config.acks.isEmpty()) {
            throw IllegalArgumentException("Kafka Broker => Broker name: ${broker.brokerName} => Acks cannot be null or empty")
        }
        if (config.retries < 0) {
            throw IllegalArgumentException("Kafka Broker => Broker name: ${broker.brokerName} => Retries cannot be less than 0")
        }
        if (config.requestTimeoutMs <= 0) {
            throw IllegalArgumentException("Kafka Broker => Broker name: ${broker.brokerName} => Request timeout must be greater than 0")
        }

        if (this.requiresSchemaRegistry && authConfig.schemaRegistryUrl.isNullOrEmpty()) {
            throw IllegalArgumentException("Kafka Broker => Broker name: ${broker.brokerName} => Schema registry URL cannot be null or empty for Avro format")
        }
    }

    /**
     * Assert requirement to use Schema Registry for Avro serialisation
     * Or if a Schema Registry URL is provided (Ie. For JSON Schemas)
     */
    private val requiresSchemaRegistry =
        (broker.valueSerializationFormat == Broker.ProducerFormat.AVRO || broker.keySerializationFormat == Broker.ProducerFormat.AVRO) || !this.authConfig.schemaRegistryUrl.isNullOrEmpty()

}