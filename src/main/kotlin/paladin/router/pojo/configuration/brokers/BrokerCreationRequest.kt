package paladin.router.pojo.configuration.brokers

import paladin.router.models.configuration.brokers.MessageBroker
import java.io.Serializable

data class BrokerCreationRequest(
    val broker: MessageBroker,
    val configuration: Map<String, Any>
): Serializable