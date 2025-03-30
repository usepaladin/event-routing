package paladin.router.models.dispatch

import org.apache.avro.specific.SpecificRecord
import paladin.router.enums.configuration.Broker
import paladin.router.models.avro.database.ChangeEventData
import paladin.router.models.avro.database.DatabaseEventRouterValueAv

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
                brokerType = Broker.Factory.fromAvro(value.brokerType),
                brokerFormat = value.brokerFormat,
                topicSchema = """""",
                payload = value.payload
            )
        }

    }
}