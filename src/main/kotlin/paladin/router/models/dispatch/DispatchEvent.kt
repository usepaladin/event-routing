package paladin.router.models.dispatch

import org.apache.avro.specific.SpecificRecord
import paladin.avro.database.ChangeEventData
import paladin.avro.database.DatabaseEventRouterValueAv
import paladin.router.enums.configuration.Broker

data class DispatchEvent <T: SpecificRecord>(
    val brokerName: String,
    val brokerType: Broker.BrokerType,
    val brokerFormat: Broker.BrokerFormat,
    val topicSchema: String,
    val payload: T
){
    companion object Factory{

        fun fromEvent(value: DatabaseEventRouterValueAv): DispatchEvent<ChangeEventData>{
            return DispatchEvent<ChangeEventData>(
                brokerName = value.brokerName,
                brokerType = Broker.fromAvro(value.brokerType),
                brokerFormat = Broker.fromAvro(value.brokerFormat),
                topicSchema = """""",
                payload = value.payload
            )
        }

    }
}