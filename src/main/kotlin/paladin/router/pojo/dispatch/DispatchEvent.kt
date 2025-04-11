package paladin.router.pojo.dispatch

import org.apache.avro.specific.SpecificRecord
import paladin.avro.database.ChangeEventData
import paladin.avro.database.DatabaseEventRouterValueAv
import paladin.router.enums.configuration.Broker

data class DispatchEvent <T: SpecificRecord>(
    val brokerName: String,
    val brokerType: Broker.BrokerType,
    val keyFormat: Broker.BrokerFormat?,
    val payloadFormat: Broker.BrokerFormat,
    val topic: String,
    val keySchema: String?,
    val payloadSchema: String?,
    val payload: T
){
    companion object Factory{

        fun fromEvent(value: DatabaseEventRouterValueAv): List<DispatchEvent<ChangeEventData>> {
            return value.brokers.map{broker ->
                DispatchEvent<ChangeEventData>(
                    brokerName = broker.brokerName,
                    brokerType = Broker.fromAvro(broker.brokerType),
                    keyFormat = Broker.fromAvro(broker.keyFormat),
                    keySchema = broker.topicKeySchema,
                    payloadFormat = Broker.fromAvro(broker.valueFormat),
                    payloadSchema = broker.topicValueSchema,
                    topic = broker.topic,
                    payload = value.payload
                )
            }
        }

    }
}