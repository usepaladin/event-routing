package paladin.router.util.factory

import org.springframework.stereotype.Service
import paladin.router.models.configuration.brokers.MessageBroker
import paladin.router.models.configuration.brokers.auth.EncryptedBrokerConfig
import paladin.router.models.configuration.brokers.auth.KafkaEncryptedConfig
import paladin.router.models.configuration.brokers.auth.RabbitEncryptedConfig
import paladin.router.models.configuration.brokers.auth.SQSEncryptedConfig
import paladin.router.models.configuration.brokers.core.KafkaBrokerConfig
import paladin.router.models.configuration.brokers.core.RabbitBrokerConfig
import paladin.router.models.configuration.brokers.core.SQSBrokerConfig
import paladin.router.models.dispatch.*
import paladin.router.models.configuration.brokers.core.BrokerConfig
import paladin.router.models.dispatch.MessageDispatcher
import paladin.router.services.schema.SchemaService

@Service
class MessageDispatcherFactory(private val schemaService: SchemaService) {
    fun fromBrokerConfig(broker: MessageBroker, config: BrokerConfig, authConfig: EncryptedBrokerConfig): MessageDispatcher {
        return when{
            config is KafkaBrokerConfig && authConfig is KafkaEncryptedConfig -> {
                KafkaDispatcher(
                    broker = broker,
                    config = config,
                    authConfig = authConfig,
                    schemaService = schemaService
                )
            }

            config is RabbitBrokerConfig && authConfig is RabbitEncryptedConfig -> {
                RabbitDispatcher(
                    broker = broker,
                    config = config,
                    authConfig = authConfig,
                    schemaService = schemaService
                )
            }

            config is SQSBrokerConfig && authConfig is SQSEncryptedConfig -> {
                SQSDispatcher(
                    broker = broker,
                    config = config,
                    authConfig = authConfig,
                    schemaService = schemaService
                )
            }

            else -> {
                throw IllegalArgumentException("Unsupported broker configuration")
            }
        }
    }

}