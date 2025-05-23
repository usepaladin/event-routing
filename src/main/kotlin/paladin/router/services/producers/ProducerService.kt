package paladin.router.services.producers

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.convertValue
import io.github.oshai.kotlinlogging.KLogger
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.stereotype.Service
import paladin.router.configuration.properties.EncryptionConfigurationProperties
import paladin.router.entities.producers.configuration.MessageProducerConfigurationEntity
import paladin.router.exceptions.ProducerNotFoundException
import paladin.router.models.configuration.producers.MessageProducer
import paladin.router.models.configuration.producers.ProducerCreationRequest
import paladin.router.models.configuration.producers.auth.EncryptedProducerConfig
import paladin.router.models.configuration.producers.core.ProducerConfig
import paladin.router.models.dispatch.MessageDispatcher
import paladin.router.repository.MessageProducerRepository
import paladin.router.services.dispatch.DispatchService
import paladin.router.services.encryption.EncryptionService
import paladin.router.util.factory.MessageDispatcherFactory
import paladin.router.util.factory.ProducerConfigFactory
import java.io.IOException


@Service
class ProducerService(
    private val producerRepository: MessageProducerRepository,
    private val dispatchService: DispatchService,
    private val encryptionService: EncryptionService,
    private val logger: KLogger,
    private val serviceEncryptionConfig: EncryptionConfigurationProperties,
    private val kafkaListenerEndpointRegistry: KafkaListenerEndpointRegistry,
    private val messageDispatcherFactory: MessageDispatcherFactory,
    private val objectMapper: ObjectMapper
) : ApplicationRunner {

    /**
     * On service start, before Kafka listeners begin to consume messages.
     * Populate all message producers from the database and builds Message dispatchers.
     * Once a dispatcher is built. Fetch all of its associated topics and register them
     * On completion, Kafka listeners will be activated to begin consuming messages and routing
     * them to their correct event producer
     */
    override fun run(args: ApplicationArguments?) {
        // After producers have been populated, allow Listeners to consume messages and route messages
        val producers = producerRepository.findAll()
        producers.forEach { entity ->
            MessageProducer.fromEntity(entity).let {
                val properties: Map<String, Any> =
                    entity.producerConfig + encryptionService.decryptObject(entity.producerConfigEncrypted)
                        .let { config ->
                            if (config == null) {
                                throw IOException("Failed to decrypt producer configuration")
                            }

                            config
                        }

                val (config: ProducerConfig, authConfig: EncryptedProducerConfig) = ProducerConfigFactory.fromConfigurationProperties(
                    it.brokerType,
                    properties
                )


                messageDispatcherFactory.fromProducerProperties(it, config, authConfig).also { dispatcher ->
                    dispatcher.validate()
                    dispatcher.build()
                    // Store the dispatcher in the dispatch service to route messages generated from other services
                    dispatchService.init(dispatcher)
                }
            }
        }

        kafkaListenerEndpointRegistry.listenerContainers.forEach { container ->
            if (!container.isRunning) {
                container.start()
            }
        }
    }

    /**
     * Generates a new message producer with predefined configurations of the specific broker type specified
     * With this generated producer, a new Message Dispatcher is created and stored in the dispatch service to route
     * messages generated from other services
     *
     * Before creation, the producer configuration is validated to ensure that the producer is fully functional with the
     * correct attached configuration and connection details and ready for
     *      * @param request [ProducerCreationReques use.
     *t] - The configuration details of the producer being added
     */
    @Throws(IllegalArgumentException::class, IOException::class)
    fun registerProducer(request: ProducerCreationRequest): MessageDispatcher {
        // Generate Configuration Classes based on specific producer type and configuration properties
        try {
            // Generate producer configuration properties
            val (producerConfig: ProducerConfig, encryptedConfig: EncryptedProducerConfig) = ProducerConfigFactory.fromConfigurationProperties(
                brokerType = request.brokerType,
                properties = request.configuration
            )

            val encryptedProducerConfig: String = serviceEncryptionConfig.requireDataEncryption.let {
                if (it) return@let encryptionService.encryptObject(encryptedConfig)
                    ?: throw IOException("Failed to encrypt broker configuration")

                encryptedConfig.toString()
            }

            val entity = MessageProducerConfigurationEntity(
                producerName = request.producerName,
                brokerType = request.brokerType,
                keyFormat = request.keySerializationFormat,
                valueFormat = request.valueSerializationFormat,
                producerConfig = objectMapper.convertValue(producerConfig),
                producerConfigEncrypted = encryptedProducerConfig
            )

            // Store the producer configuration in the database
            val savedBroker: MessageProducerConfigurationEntity = producerRepository.save(entity)
            MessageProducer.fromEntity(savedBroker).run {
                return messageDispatcherFactory.fromProducerProperties(
                    producer = this,
                    config = producerConfig,
                    connectionConfig = encryptedConfig
                ).also {
                    // Validate the dispatcher to ensure that the producer is fully functional, and throw an exception if any errors occur
                    it.validate()
                    it.build()

                    logger.info { "Producer Service => Producer ${this.producerName} created successfully" }
                    dispatchService.setDispatcher(
                        this.producerName,
                        it
                    )
                }
            }

        } catch (e: Exception) {
            logger.error { "Producer Service => An error occurred when parsing producer configurations for Broker type: ${request.brokerType} => Message: ${e.message}" }
            throw e
        }
    }

    /**
     * Removes the producer from the database, and as an active message dispatcher.
     * It will be assumed that the delete functionality will not be accessible until all references
     * to this producer have been removed, and moved to another producer to avoid message loss
     *
     * @param name [String] - The name of the producer to be deleted
     * @throws IllegalArgumentException - If the producer does not exist
     *
     */
    fun deleteProducer(name: String): Boolean {
        try {
            val dispatcher: MessageDispatcher = dispatchService.getDispatcher(name)
                ?: throw ProducerNotFoundException("Dispatcher not found with name $name")

            // Remove the producer from the database
            producerRepository.deleteById(dispatcher.producer.id).run {
                logger.info { "Producer Service => Producer $name deleted successfully" }
                // Remove the dispatcher from the dispatch service to stop routing messages generated from other services
                dispatchService.removeDispatcher(name)
            }
            return true
        } catch (ex: Exception) {
            logger.error { "Producer Service => An error occurred when deleting producer of name: $name => Message: ${ex.message}" }
            return false
        }
    }
}