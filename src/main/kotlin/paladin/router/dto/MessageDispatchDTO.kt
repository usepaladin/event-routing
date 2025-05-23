package paladin.router.dto

import paladin.router.models.dispatch.MessageDispatcher

data class MessageDispatchDTO(
    val broker: ProducerDTO,
    val connectionState: MessageDispatcher.MessageDispatcherState
) {
    companion object Factory {
        fun fromEntity(dispatcher: MessageDispatcher) = MessageDispatchDTO(
            broker = ProducerDTO.fromEntity(
                producer = dispatcher.producer,
                config = dispatcher.producerConfig,
                connectionConfig = dispatcher.connectionConfig,
            ),
            connectionState = dispatcher.getConnectionState(),
        )
    }
}