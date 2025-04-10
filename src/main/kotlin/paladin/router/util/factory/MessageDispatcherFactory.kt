package paladin.router.util.factory

import org.apache.avro.generic.GenericRecord
import org.springframework.stereotype.Service
import paladin.router.enums.configuration.Broker
import paladin.router.models.configuration.brokers.MessageBroker
import paladin.router.models.dispatch.*
import paladin.router.pojo.configuration.brokers.auth.*
import paladin.router.pojo.configuration.brokers.core.BrokerConfig
import paladin.router.pojo.configuration.brokers.core.*
import paladin.router.pojo.dispatch.MessageDispatcher
import paladin.router.services.schema.SchemaService

@Service
class MessageDispatcherFactory(private val schemaService: SchemaService) {
    fun fromBrokerConfig(broker: MessageBroker, config: BrokerConfig, authConfig: EncryptedBrokerConfig): MessageDispatcher{
        return when{
            config is KafkaBrokerConfig && authConfig is KafkaEncryptedConfig -> {
                getSpecificKafkaDispatcher(broker, authConfig, config)
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

    private fun getSpecificKafkaDispatcher(broker: MessageBroker, authConfig: KafkaEncryptedConfig, config: KafkaBrokerConfig): KafkaDispatcher<*,*>{
        return when(broker.keySerializationFormat){
            Broker.BrokerFormat.STRING, Broker.BrokerFormat.JSON, null -> {
                val keyDispatcher = KafkaDispatcher<String, Any>(
                    broker = broker,
                    config = config,
                    authConfig = authConfig,
                    schemaService = schemaService
                )

                populateValueDispatchFormat(keyDispatcher)
            }
            Broker.BrokerFormat.AVRO -> {
                val keyDispatcher = KafkaDispatcher<String, Any>(
                    broker = broker,
                    config = config,
                    authConfig = authConfig,
                    schemaService = schemaService
                )

                populateValueDispatchFormat(keyDispatcher)
            }
        }
    }

    private fun <T> populateValueDispatchFormat(dispatcher: KafkaDispatcher<T, *>): KafkaDispatcher<T, *>{
        return when(dispatcher.broker.valueSerializationFormat){
            Broker.BrokerFormat.STRING, Broker.BrokerFormat.JSON -> {
                KafkaDispatcher<T, String>(dispatcher)
            }
            Broker.BrokerFormat.AVRO -> {
                KafkaDispatcher<T, GenericRecord>(dispatcher)
            }
        }

    }

}