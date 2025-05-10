package paladin.router.models.configuration.brokers

import paladin.router.enums.configuration.Broker

import java.io.Serializable

data class BrokerCreationRequest(
    val brokerName: String,
    val brokerType: Broker.BrokerType,
    val keySerializationFormat: Broker.BrokerFormat?,
    val valueSerializationFormat: Broker.BrokerFormat,
    val defaultBroker: Boolean,
    val configuration: Map<String, Any>
): Serializable