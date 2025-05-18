package paladin.router.models.configuration.producers

import paladin.router.enums.configuration.Broker
import java.io.Serializable

data class ProducerCreationRequest(
    val producerName: String,
    val brokerType: Broker.BrokerType,
    val keySerializationFormat: Broker.ProducerFormat?,
    val valueSerializationFormat: Broker.ProducerFormat,
    val configuration: Map<String, Any?>
) : Serializable