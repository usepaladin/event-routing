package paladin.router.models.listener

import paladin.router.enums.configuration.Broker
import java.io.Serializable

data class ListenerRegistrationRequest(
    val topic: String,
    val groupId: String,
    val runOnStartup: Boolean = false,
    val key: Broker.ProducerFormat,
    val value: Broker.ProducerFormat,
    val config: AdditionalConsumerProperties,
) : Serializable

