package paladin.router.services.brokers

import io.github.oshai.kotlinlogging.KLogger
import org.springframework.stereotype.Service
import paladin.router.configuration.properties.EncryptionConfigurationProperties
import paladin.router.entities.brokers.configuration.MessageBrokerConfigurationEntity
import paladin.router.enums.configuration.Broker
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
    private val serviceEncryptionConfig: EncryptionConfigurationProperties
    ) {

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
            val dispatcher: MessageDispatcher = MessageDispatcherFactory.fromBrokerConfig(
                broker = broker,
                config = brokerConfig,
                authConfig = encryptedConfig
            )

            // Validate the dispatcher to ensure that the broker is fully functional, and throw an exception if any errors occur
            dispatcher.validate()

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

    fun updateBrokerConfiguraton(){

    }
    fun removeBroker(broker: String){
        dispatchService.removeDispatcher(broker)
    }

}