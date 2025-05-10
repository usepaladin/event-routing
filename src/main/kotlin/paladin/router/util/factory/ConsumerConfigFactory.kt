package paladin.router.util.factory

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Deserializer
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
import paladin.router.enums.configuration.Broker
import paladin.router.models.listener.EventListener

object ConsumerConfigFactory {
    /**
     * Generates Configuration properties for an Event Listener being registered
     */
    fun buildConsumer(
        config: MutableMap<String, Any>,
        eventListener: EventListener
    ): Triple<Map<String, Any>, Deserializer<*>, Deserializer<*>> {
        val keyDeserializer: ErrorHandlingDeserializer<*> =
            generateDeserializer(eventListener.key, eventListener.config.schemaRegistryUrl, true)
        val valueDeserializer = generateDeserializer(eventListener.value, eventListener.config.schemaRegistryUrl)

        // Configure the deserializers
        handleDeserializerConfiguration(eventListener, config, eventListener.key, true)
        handleDeserializerConfiguration(eventListener, config, eventListener.value, false)

        applyConfigIfExist(
            config,
            listOf(
                ConsumerConfig.GROUP_ID_CONFIG to eventListener.groupId,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to eventListener.config.enableAutoCommit,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to eventListener.config.autoOffsetReset,
                ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG to eventListener.config.requestTimeoutMs,
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG to eventListener.config.maxPollRecords,
                ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG to eventListener.config.maxPollIntervalMs,
                ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG to eventListener.config.sessionTimeoutMs,
                ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG to eventListener.config.requestTimeoutMs,
                ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG to eventListener.config.autoCommitIntervalMs,
            )
        )

        // Set the deserializers in the configuration
        config[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = keyDeserializer
        config[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = valueDeserializer

        return Triple(
            config,
            keyDeserializer,
            valueDeserializer
        )
    }

    private fun handleDeserializerConfiguration(
        eventListener: EventListener,
        config: MutableMap<String, Any>,
        format: Broker.BrokerFormat,
        isKey: Boolean
    ) {
        when (format) {
            Broker.BrokerFormat.JSON -> {
                val prefix = if (isKey) "spring.json.key." else "spring.json.value."
                config["${prefix}trusted.packages"] = "*"
                config["${prefix}use.type.headers"] = true
            }

            Broker.BrokerFormat.AVRO -> {
                eventListener.config.schemaRegistryUrl.let {
                    if (it.isNullOrEmpty()) {
                        throw IllegalArgumentException("Schema Registry URL is required for AVRO format")
                    }

                    config["schema.registry.url"] = it
                    config["specific.avro.reader"] = true
                }
            }

            else -> return
        }
    }

    /**
     * Creates a deserializer instance for the specified format with error handling wrapped around it
     */
    private fun generateDeserializer(
        format: Broker.BrokerFormat,
        schemaRegistryUrl: String? = null,
        isKey: Boolean = false
    ): ErrorHandlingDeserializer<*> {
        val baseDeserializer = when (format) {
            Broker.BrokerFormat.STRING -> org.apache.kafka.common.serialization.StringDeserializer()
            Broker.BrokerFormat.JSON -> {
                schemaRegistryUrl.let {
                    if (it.isNullOrEmpty()) {
                        return@let org.springframework.kafka.support.serializer.JsonDeserializer<Any>().apply {
                            this.addTrustedPackages("*")
                            this.setUseTypeHeaders(true)
                        }
                    }
                    // If schema registry URL is provided, use KafkaJsonDeserializer
                    return@let io.confluent.kafka.serializers.KafkaJsonDeserializer<Any>().apply {
                        configure(
                            mapOf(
                                "schema.registry.url" to schemaRegistryUrl,
                                "specific.avro.reader" to true
                            ), isKey
                        )
                    }
                }
            }

            Broker.BrokerFormat.AVRO -> {
                // Check if schema registry URL is provided
                schemaRegistryUrl.let {
                    if (it.isNullOrEmpty()) {
                        throw IllegalArgumentException("Schema Registry URL is required for AVRO format")
                    }

                    io.confluent.kafka.serializers.KafkaAvroDeserializer().apply {
                        configure(
                            mapOf(
                                "schema.registry.url" to schemaRegistryUrl,
                                "specific.avro.reader" to true
                            ), isKey
                        )
                    }
                }


            }
        }

        // Wrap in error handling deserializer
        return ErrorHandlingDeserializer(baseDeserializer)
    }

    private fun applyConfigIfExist(
        config: MutableMap<String, Any>,
        values: List<Pair<String, Any?>>,
    ) {
        values.forEach { (key, value) ->
            value?.let {
                config[key] = it
            }
        }
    }
}