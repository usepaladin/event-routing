package paladin.router.models.dispatch

import paladin.router.enums.configuration.Broker
import java.io.Serializable

data class DispatchTopicRequest(
    val dispatcher: String,
    val sourceTopic: String,
    val destinationTopic: String,
    val key: Broker.BrokerFormat,
    val keySchema: String? = null,
    val value: Broker.BrokerFormat,
    val valueSchema: String? = null
) : Serializable