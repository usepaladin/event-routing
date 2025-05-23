package paladin.router.util.factory

import io.micrometer.core.instrument.MeterRegistry
import org.springframework.stereotype.Component
import paladin.router.models.configuration.producers.MessageProducer
import paladin.router.models.configuration.producers.auth.EncryptedProducerConfig
import paladin.router.models.configuration.producers.auth.KafkaEncryptedConfig
import paladin.router.models.configuration.producers.auth.RabbitEncryptedConfig
import paladin.router.models.configuration.producers.auth.SQSEncryptedConfig
import paladin.router.models.configuration.producers.core.KafkaProducerConfig
import paladin.router.models.configuration.producers.core.ProducerConfig
import paladin.router.models.configuration.producers.core.RabbitProducerConfig
import paladin.router.models.configuration.producers.core.SQSProducerConfig
import paladin.router.models.dispatch.KafkaDispatcher
import paladin.router.models.dispatch.MessageDispatcher
import paladin.router.models.dispatch.RabbitDispatcher
import paladin.router.models.dispatch.SqsDispatcher
import paladin.router.services.schema.SchemaService

@Component
class MessageDispatcherFactory(private val schemaService: SchemaService, private val meterRegistry: MeterRegistry) {
    fun fromProducerProperties(
        producer: MessageProducer,
        config: ProducerConfig,
        connectionConfig: EncryptedProducerConfig
    ): MessageDispatcher {
        return when {
            config is KafkaProducerConfig && connectionConfig is KafkaEncryptedConfig -> {
                KafkaDispatcher(
                    producer = producer,
                    producerConfig = config,
                    connectionConfig = connectionConfig,
                    schemaService = schemaService,
                    meterRegistry = meterRegistry
                )
            }

            config is RabbitProducerConfig && connectionConfig is RabbitEncryptedConfig -> {
                RabbitDispatcher(
                    producer = producer,
                    producerConfig = config,
                    connectionConfig = connectionConfig,
                    schemaService = schemaService,
                    meterRegistry = meterRegistry
                )
            }

            config is SQSProducerConfig && connectionConfig is SQSEncryptedConfig -> {
                SqsDispatcher(
                    producer = producer,
                    producerConfig = config,
                    connectionConfig = connectionConfig,
                    schemaService = schemaService,
                    meterRegistry = meterRegistry
                )
            }

            else -> {
                throw IllegalArgumentException("Unsupported broker configuration")
            }
        }
    }

}