package paladin.router.models.dispatch

import paladin.router.enums.configuration.Broker

data class DispatchTopicRequest(
    val dispatcher: String,
    val topic: String,
    val key: Broker.BrokerFormat,
    val keySchema: String? = null,
    val value: Broker.BrokerFormat,
    val valueSchema: String? = null
)