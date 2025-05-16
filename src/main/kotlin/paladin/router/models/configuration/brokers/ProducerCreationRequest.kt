package paladin.router.models.configuration.brokers

import paladin.router.enums.configuration.Broker

import java.io.Serializable

data class ProducerCreationRequest(
    val brokerName: String,
    val brokerType: Broker.BrokerType,
    val keySerializationFormat: Broker.ProducerFormat?,
    val valueSerializationFormat: Broker.ProducerFormat,
    val defaultBroker: Boolean,
    val configuration: Map<String, Any>
) : Serializable