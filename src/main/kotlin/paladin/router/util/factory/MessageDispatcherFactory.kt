package paladin.router.util.factory

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.shaded.com.google.protobuf.DynamicMessage
import paladin.router.enums.configuration.Broker
import paladin.router.models.configuration.brokers.MessageBroker
import paladin.router.models.dispatch.*
import paladin.router.pojo.configuration.brokers.auth.*
import paladin.router.pojo.configuration.brokers.core.BrokerConfig
import paladin.router.pojo.configuration.brokers.core.*
import paladin.router.pojo.dispatch.MessageDispatcher

object MessageDispatcherFactory {
    fun fromBrokerConfig(broker: MessageBroker, config: BrokerConfig, authConfig: EncryptedBrokerConfig): MessageDispatcher{
        return when{
            config is KafkaBrokerConfig && authConfig is KafkaEncryptedConfig -> {
                getSpecificKafkaDispatcher(broker, authConfig, config)
            }

            config is RabbitBrokerConfig && authConfig is RabbitEncryptedConfig -> {
                RabbitDispatcher(
                    broker = broker,
                    config = config,
                    authConfig = authConfig
                )
            }

            config is SQSBrokerConfig && authConfig is SQSEncryptedConfig -> {
                SQSDispatcher(
                    broker = broker,
                    config = config,
                    authConfig = authConfig
                )
            }

            config is MQTTBrokerConfig && authConfig is MQTTEncryptedConfig -> {
                MQTTDispatcher(
                    broker = broker,
                    config = config,
                    authConfig = authConfig
                )
            }

            config is PulsarBrokerConfig && authConfig is PulsarEncryptedConfig -> {
                getSpecificPulsarDispatcher(broker, authConfig, config)
            }

            else -> {
                throw IllegalArgumentException("Unsupported broker configuration")
            }
        }
    }

    private fun getSpecificKafkaDispatcher(broker: MessageBroker, authConfig: KafkaEncryptedConfig, config: KafkaBrokerConfig): KafkaDispatcher<*,*>{
        return when(broker.brokerFormat){
            Broker.BrokerFormat.AVRO -> {
                KafkaDispatcher<String, GenericRecord>(
                    broker = broker,
                    config = config,
                    authConfig = authConfig
                )
            }
            Broker.BrokerFormat.JSON -> {
                KafkaDispatcher<String, String>(
                    broker = broker,
                    config = config,
                    authConfig = authConfig
                )
            }
            Broker.BrokerFormat.PROTOBUF -> {
                KafkaDispatcher<String, DynamicMessage>(
                    broker = broker,
                    config = config,
                    authConfig = authConfig
                )
            }
        }
    }

    private fun getSpecificPulsarDispatcher(broker: MessageBroker, authConfig: PulsarEncryptedConfig, config: PulsarBrokerConfig): PulsarDispatcher<*>{
        return when(broker.brokerFormat){
            Broker.BrokerFormat.AVRO -> {
                PulsarDispatcher<GenericRecord>(
                    broker = broker,
                    config = config,
                    authConfig = authConfig
                )
            }
            Broker.BrokerFormat.JSON -> {
                PulsarDispatcher<String>(
                    broker = broker,
                    config = config,
                    authConfig = authConfig
                )
            }
            Broker.BrokerFormat.PROTOBUF -> {
                PulsarDispatcher<DynamicMessage>(
                    broker = broker,
                    config = config,
                    authConfig = authConfig
                )
            }
        }
    }
}