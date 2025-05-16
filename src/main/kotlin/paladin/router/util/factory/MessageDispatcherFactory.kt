package paladin.router.util.factory

import org.springframework.stereotype.Component
import paladin.router.models.configuration.brokers.MessageProducer
import paladin.router.models.configuration.brokers.auth.EncryptedProducerConfig
import paladin.router.models.configuration.brokers.auth.KafkaEncryptedConfig
import paladin.router.models.configuration.brokers.auth.RabbitEncryptedConfig
import paladin.router.models.configuration.brokers.auth.SQSEncryptedConfig
import paladin.router.models.configuration.brokers.core.KafkaProducerConfig
import paladin.router.models.configuration.brokers.core.RabbitProducerConfig
import paladin.router.models.configuration.brokers.core.SQSProducerConfig
import paladin.router.models.dispatch.*
import paladin.router.models.configuration.brokers.core.ProducerConfig
import paladin.router.models.dispatch.MessageDispatcher
import paladin.router.services.schema.SchemaService

@Component
class MessageDispatcherFactory(private val schemaService: SchemaService) {
    fun fromBrokerConfig(
        broker: MessageProducer,
        config: ProducerConfig,
        authConfig: EncryptedProducerConfig
    ): MessageDispatcher {
        return when {
            config is KafkaProducerConfig && authConfig is KafkaEncryptedConfig -> {
                KafkaDispatcher(
                    broker = broker,
                    config = config,
                    authConfig = authConfig,
                    schemaService = schemaService
                )
            }

            config is RabbitProducerConfig && authConfig is RabbitEncryptedConfig -> {
                RabbitDispatcher(
                    broker = broker,
                    config = config,
                    authConfig = authConfig,
                    schemaService = schemaService
                )
            }

            config is SQSProducerConfig && authConfig is SQSEncryptedConfig -> {
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