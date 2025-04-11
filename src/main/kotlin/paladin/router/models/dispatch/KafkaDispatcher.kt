package paladin.router.models.dispatch

import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord

import paladin.router.enums.configuration.Broker
import paladin.router.models.configuration.brokers.MessageBroker
import paladin.router.pojo.configuration.brokers.auth.KafkaEncryptedConfig
import paladin.router.pojo.configuration.brokers.core.KafkaBrokerConfig
import paladin.router.pojo.dispatch.MessageDispatcher
import paladin.router.services.schema.SchemaService
import java.util.Properties


data class KafkaDispatcher <T, P>(
    override val broker: MessageBroker,
    override val config: KafkaBrokerConfig,
    override val authConfig: KafkaEncryptedConfig,
    override val schemaService: SchemaService
): MessageDispatcher()  {
    constructor(dispatcher: KafkaDispatcher<*,*>) : this(
        broker = dispatcher.broker,
        config = dispatcher.config,
        authConfig = dispatcher.authConfig,
        schemaService = dispatcher.schemaService
    )
    private var producer: KafkaProducer<T,P>? = null
    override val logger: KLogger
        get() = KotlinLogging.logger {  }

    override fun <K, V> dispatch(topic: String, key: K, payload: V, keySchema: String?, payloadSchema: String?) {
        if(producer == null){
            logger.error { "Kafka Broker => Broker name: ${broker.brokerName} => Unable to send message => Producer has not been instantiated" }
            return;
        }

        val (parsedKey: T, parsedPayload: P) = parseMessageValues(key, payload)
        val record: ProducerRecord<T,P> = ProducerRecord(topic, parsedKey, parsedPayload)
        try {
            producer?.send(record)?.get()
            logger.info { "Kafka Broker => Broker name: ${broker.brokerName} => Message sent successfully to topic: $topic" }
        } catch (e: Exception) {
            logger.error(e) { "Kafka Broker => Broker name: ${broker.brokerName} => Error sending message to topic: $topic" }
        }

    }

    override fun <V> dispatch(topic: String, payload: V, payloadSchema: String?) {
        throw UnsupportedOperationException("Kafka does not support dispatching without a key")
    }

    override fun build() {
        this.updateConnectionState(MessageDispatcherState.Building)
        val properties = Properties()
        properties.apply {
            put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, authConfig.bootstrapServers)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, getSerializerClass(broker.keySerializationFormat
                ?: Broker.BrokerFormat.STRING))
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, getSerializerClass(broker.valueSerializationFormat))
            put(ProducerConfig.ACKS_CONFIG, config.acks)
            put(ProducerConfig.RETRIES_CONFIG, config.retries)
            put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, config.requestTimeoutMs)
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
    }

    /**
     * Converts the key and value to the appropriate type based on the broker configuration.
     * Also utilises any provided schema to parse the message (When using Json or Avro)
     *
     * @param key The key of the message
     * @param value The value of the message
     *
     * @return A pair of the parsed key and value
     */
    private fun <K, V> parseMessageValues(key: K, value: V, keySchema: String? = null, payloadSchema: String? = null): Pair<T,P> {
        val parsedKey = convertToFormat(key, broker.keySerializationFormat ?: Broker.BrokerFormat.STRING, keySchema)
        val parsedValue = convertToFormat(value, broker.valueSerializationFormat, payloadSchema)

        // dont care didnt ask for a warning cry about it
        return Pair(parsedKey as T, parsedValue as P)
    }

    private fun <T> convertToFormat(value: T, format: Broker.BrokerFormat, schema: String?): Any {
        return when(format){
            Broker.BrokerFormat.STRING -> schemaService.parseToString(value)
            Broker.BrokerFormat.JSON -> {
                if(schema == null) {
                    return schemaService.parseToJson(value)

                }
                return schemaService.parseToJson(schema, value)
            }
            Broker.BrokerFormat.AVRO -> {
                if(schema == null) {
                    throw IllegalArgumentException("Schema cannot be null for Avro format")
                }
                return schemaService.parseToAvro(schema, value)
            }
        }
    }

    private fun getSerializerClass(brokerFormat: Broker.BrokerFormat): String{
        return when(brokerFormat){
            Broker.BrokerFormat.STRING -> "org.apache.kafka.common.serialization.StringSerializer"
            Broker.BrokerFormat.JSON -> "org.springframework.kafka.support.serializer.JsonSerializer"
            Broker.BrokerFormat.AVRO -> "io.confluent.kafka.serializers.KafkaAvroSerializer"
        }
    }

}