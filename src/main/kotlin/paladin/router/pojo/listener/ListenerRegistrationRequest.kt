package paladin.router.pojo.listener

import paladin.router.enums.configuration.Broker
import java.io.Serializable

data class ListenerRegistrationRequest(
    val topic: String,
    val groupId: String,
    val key: PayloadFormat,
    val value: PayloadFormat,
): Serializable

data class PayloadFormat(
    val format: Broker.BrokerFormat,
    val schema: String? = null
)
