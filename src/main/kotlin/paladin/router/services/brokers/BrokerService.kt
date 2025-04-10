package paladin.router.services.brokers

import io.github.oshai.kotlinlogging.KLogger
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.stereotype.Service
import paladin.router.configuration.properties.EncryptionConfigurationProperties
import paladin.router.entities.brokers.configuration.MessageBrokerConfigurationEntity
import paladin.router.exceptions.BrokerNotFoundException
import paladin.router.models.configuration.brokers.MessageBroker
import paladin.router.pojo.configuration.brokers.auth.EncryptedBrokerConfig
import paladin.router.pojo.configuration.brokers.core.BrokerConfig
import paladin.router.pojo.dispatch.MessageDispatcher
import paladin.router.repository.MessageBrokerRepository
import paladin.router.services.dispatch.DispatchService
import paladin.router.services.encryption.EncryptionService
import paladin.router.util.factory.BrokerConfigFactory
import paladin.router.util.factory.MessageDispatcherFactory
import java.io.IOException
import kotlin.jvm.Throws


@Service
class BrokerService(
    private val messageBrokerRepository: MessageBrokerRepository,
    private val dispatchService: DispatchService,
    private val encryptionService: EncryptionService,
    private val logger: KLogger,
    private val serviceEncryptionConfig: EncryptionConfigurationProperties,
    private val kafkaListenerEndpointRegistry: KafkaListenerEndpointRegistry,
    private val messageDispatcherFactory: MessageDispatcherFactory
    ): ApplicationRunner {

    /**
     * On service start, before Kafka listeners begin to consume messages. The application
     * will populate all message brokers from the database and build Message dispatchers.
     */
    override fun run(args: ApplicationArguments?) {
        populateDispatchers()
    }

    /**
     * Populates all message brokers from the database and builds Message dispatchers.
     * On completion, Kafka listeners will be activated to begin consuming messages and routing
     * them to their correct event broker
     */
    private fun populateDispatchers(): Unit{
        // After brokers have been populated, allow Listeners to consume messages and route messages
        val brokers: List<MessageBrokerConfigurationEntity> = messageBrokerRepository.findAll()
        brokers.forEach { entity ->
            val broker: MessageBroker = MessageBroker.fromEntity(entity)
            val encryptedConfig: Map<String, Any> = encryptionService.decryptObject(entity.brokerConfigEncrypted)?:
                throw IOException("Failed to decrypt broker configuration for broker: ${broker.brokerName}")
            // Conjoin properties and pass through Broker config factory
            val properties: Map<String, Any> = entity.brokerConfig + encryptedConfig
            val (config: BrokerConfig, authConfig: EncryptedBrokerConfig)  = BrokerConfigFactory.fromConfigurationProperties(entity.brokerType, properties)
            val messageDispatcher: MessageDispatcher = messageDispatcherFactory.fromBrokerConfig(broker, config, authConfig)
            // Store the dispatcher in the dispatch service to route messages generated from other services
            dispatchService.setDispatcher(
                broker.brokerName,
                messageDispatcher
            )
        }

        activateListeners()
    }

    /**
     * Starts up all Kafka listeners which will allow them to begin consuming all messages
     * within the queue and route them to their correct event broker
     */
    private fun activateListeners(){
        kafkaListenerEndpointRegistry.listenerContainers.forEach { container ->
            if (!container.isRunning) {
                container.start()
            }
        }
    }

    /**
     * Generates a new message broker with predefined configurations of the specific broker type specified
     * With this generated broker, a new Message Dispatcher is created and stored in the dispatch service to route
     * messages generated from other services
     *
     * Before creation, the broker configuration is validated to ensure that the broker is fully functional with the
     * correct attached configuration and connection details and ready for use.
     *
     * @param configuration Map<String, Any> - The configuration of the broker to be created
     * @param broker MessageBroker - The core details of the message broker being created
     */
    @Throws(IllegalArgumentException::class, IOException::class)
    fun createBroker(broker: MessageBroker, configuration: Map<String, Any>): MessageDispatcher{
        // Generate Configuration Classes based on specific broker type and configuration properties
        try{
            // Generate broker configuration properties
            val (brokerConfig: BrokerConfig, encryptedConfig: EncryptedBrokerConfig) = BrokerConfigFactory.fromConfigurationProperties(
            brokerType = broker.brokerType,
            properties = configuration)

            // Generate Message Dispatcher based on broker configuration
            val dispatcher: MessageDispatcher = messageDispatcherFactory.fromBrokerConfig(
                broker = broker,
                config = brokerConfig,
                authConfig = encryptedConfig
            )
            // Validate the dispatcher to ensure that the broker is fully functional, and throw an exception if any errors occur
            dispatcher.validate()
            dispatcher.build()
            // Encrypt relevant broker configuration properties, and format broker object for database storage
            val encryptedBrokerConfig:String = if(serviceEncryptionConfig.requireDataEncryption){
                encryptionService.encryptObject(encryptedConfig)?: throw IOException("Failed to encrypt broker configuration")
            } else {
                brokerConfig.toString()
            }

            val brokerEntity = MessageBrokerConfigurationEntity.fromConfiguration(
                messageBroker = broker,
                encryptedConfig = encryptedBrokerConfig,
                brokerConfig = brokerConfig
            )

            // Store the broker configuration in the database
            messageBrokerRepository.save(brokerEntity)
            logger.info { "Broker Service => Broker ${broker.brokerName} created successfully" }

            // Store the dispatcher in the dispatch service to route messages generated from other services
            dispatchService.setDispatcher(
                broker.brokerName,
                dispatcher
            )

            return dispatcher
        }
        catch (e: Exception){
            logger.error { "Broker Service => An error occurred when parsing broker configurations for Broker type: ${broker.brokerType} => Message: ${e.message}" }
            throw e
        }
    }

    /**
     * Updates a Message broker of a particular specified type, with updated broker configuration properties
     * relevant to that specific broker. Will then re-validate the broker's configuration to ensure
     * that the broker is fully functional with the correct attached configuration and connection details
     *
     * @param updatedDispatcher - The message dispatcher with updated configuration settings to be applied and saved
     * in the database

     */
    fun updateBroker(updatedDispatcher: MessageDispatcher): MessageDispatcher{
        // Disconnect existing dispatcher to avoid sending messages with incorrect message configuration
        val dispatcher: MessageDispatcher = dispatchService.getDispatcher(updatedDispatcher.broker.brokerName)
            ?: throw BrokerNotFoundException("Dispatcher not found for broker: ${updatedDispatcher.broker.brokerName}")

        dispatcher.updateConnectionState(MessageDispatcher.MessageDispatcherState.Disconnected)

        // Update Broker and associated configuration properties
        // Dispatch properties must be static to adhere to inheritance, meaning internal properties need to be individually updated
        dispatcher.broker.updateConfiguration(updatedDispatcher.broker)
        dispatcher.config.updateConfiguration(updatedDispatcher.config)
        dispatcher.authConfig.updateConfiguration(updatedDispatcher.authConfig)

        dispatcher.validate()
        // Rebuild Producer with updated Properties,
        dispatcher.build()

        val currentState: MessageDispatcher.MessageDispatcherState = dispatcher.connectionState.value
        if(currentState is MessageDispatcher.MessageDispatcherState.Error){
            // Throw the exception that was generated during connection failure
            throw IOException("Failed to connect to broker: ${dispatcher.broker.brokerName} => Error message: ${currentState.exception.message}" )
        }

        // Save updated configuration properties to database
        val encryptedBrokerConfig: String = if(serviceEncryptionConfig.requireDataEncryption){
            encryptionService.encryptObject(updatedDispatcher.authConfig)?: throw IOException("Failed to encrypt broker configuration")
        } else {
            updatedDispatcher.config.toString()
        }

        val brokerEntity = MessageBrokerConfigurationEntity.fromConfiguration(
            messageBroker = updatedDispatcher.broker,
            encryptedConfig = encryptedBrokerConfig,
            brokerConfig = updatedDispatcher.config
        )

        // Store the broker configuration in the database
        messageBrokerRepository.save(brokerEntity)
        logger.info { "Broker Service => Broker ${updatedDispatcher.broker.brokerName} updated successfully" }

        // Store the dispatcher in the dispatch service to route messages generated from other services
        dispatchService.setDispatcher(
            updatedDispatcher.broker.brokerName,
            dispatcher
        )

        return dispatcher
    }

    /**
     * Removes the broker from the database, and as an active message dispatcher.
     * It will be assumed that the delete functionality will not be accessible until all references
     * to this broker have been removed, and moved to another broker to avoid message loss
     *
     * @param brokerName String - The name of the broker to be deleted
     *
     * @throws IllegalArgumentException - If the broker does not exist
     *
     */
    fun deleteBroker(brokerName: String): Boolean{
        try{

        val dispatcher: MessageDispatcher = dispatchService.getDispatcher(brokerName)
            ?: throw BrokerNotFoundException("Dispatcher not found for broker: $brokerName")

        // Remove the broker from the database
        messageBrokerRepository.deleteById(dispatcher.broker.id)
        logger.info { "Broker Service => Broker $brokerName deleted successfully" }
        // Remove the dispatcher from the dispatch service to stop routing messages generated from other services
        dispatchService.removeDispatcher(brokerName)
        return true
        } catch (ex: Exception){
            logger.error { "Broker Service => An error occurred when deleting broker $brokerName => Message: ${ex.message}" }
            return false
        }
    }
}