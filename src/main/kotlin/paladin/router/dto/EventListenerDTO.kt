package paladin.router.dto

import paladin.router.enums.configuration.Broker
import paladin.router.models.listener.EventListener

data class EventListenerDTO(
    val topic: String,
    val groupId: String,
    val key: Broker.ProducerFormat,
    val value: Broker.ProducerFormat,
    val dispatchers: List<MessageDispatchDTO> = emptyList(),
) {
    companion object Factory {
        fun from(listener: EventListener): EventListenerDTO {
            return EventListenerDTO(
                topic = listener.topic,
                groupId = listener.groupId,
                key = listener.key,
                value = listener.value,
            )
        }
    }
}