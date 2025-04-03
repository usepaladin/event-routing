package paladin.router.util.factory

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.shaded.com.google.protobuf.DynamicMessage
import paladin.router.enums.configuration.Broker
import paladin.router.models.configuration.brokers.MessageBroker
import paladin.router.models.dispatch.*
import paladin.router.pojo.configuration.brokers.*
import paladin.router.pojo.dispatch.MessageDispatcher

object MessageDispatcherFactory {

    fun fromBrokerConfig(broker: MessageBroker, authConfig: EncryptedBrokerAuthConfig, config: BrokerConfig): MessageDispatcher{
        when(config){
            is SQSBrokerConfig -> {
                return SQSDispatcher(
                    broker = broker,
                    config = config,
                    authConfig = authConfig
                )
            }
            is KafkaBrokerConfig -> {
                return getSpecificKafkaDispatcher(broker, authConfig, config)
            }
            is RabbitBrokerConfig -> {
                return RabbitDispatcher(
                    broker = broker,
                    config = config,
                    authConfig = authConfig
                )
            }
            is MQTTBrokerConfig -> {
                return MQTTDispatcher(
                    broker = broker,
                    config = config,
                    authConfig = authConfig
                )
            }
            is PulsarBrokerConfig -> {
                return getSpecificPulsarDispatcher(broker, authConfig, config)
            }
            else -> {
                throw IllegalArgumentException("Unsupported broker config type")
            }
        }
    }

    private fun getSpecificKafkaDispatcher(broker: MessageBroker, authConfig: EncryptedBrokerAuthConfig, config: KafkaBrokerConfig ): KafkaDispatcher<*,*>{
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

    private fun getSpecificPulsarDispatcher(broker: MessageBroker, authConfig: EncryptedBrokerAuthConfig, config: PulsarBrokerConfig): PulsarDispatcher<*>{
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