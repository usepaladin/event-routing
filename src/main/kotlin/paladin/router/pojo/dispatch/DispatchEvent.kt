package paladin.router.pojo.dispatch

import org.apache.avro.specific.SpecificRecord
import paladin.avro.database.ChangeEventData
import paladin.avro.database.DatabaseEventRouterValueAv
import paladin.router.enums.configuration.Broker

data class DispatchEvent <T: SpecificRecord>(
    val brokerName: String,
    val brokerType: Broker.BrokerType,
    val brokerFormat: Broker.BrokerFormat,
    val topic: String,
    val payloadSchema: String,
    val payload: T
){
    companion object Factory{

        fun fromEvent(value: DatabaseEventRouterValueAv): DispatchEvent<ChangeEventData> {
            return DispatchEvent<ChangeEventData>(
                brokerName = value.brokerName,
                brokerType = Broker.fromAvro(value.brokerType),
                brokerFormat = Broker.fromAvro(value.brokerFormat),
                topic = value.topic,
                payloadSchema = value.topicSchema,
                payload = value.payload
            )
        }

    }
}