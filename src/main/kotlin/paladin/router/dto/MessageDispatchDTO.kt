package paladin.router.dto

import paladin.router.models.dispatch.MessageDispatcher

data class MessageDispatchDTO(
    val broker: BrokerDTO,
    val connectionState: MessageDispatcher.MessageDispatcherState
){
    companion object Factory{
        fun fromEntity(dispatcher: MessageDispatcher) = MessageDispatchDTO(
            broker = BrokerDTO.fromEntity(
                broker = dispatcher.broker,
                config = dispatcher.config,
                authConfig = dispatcher.authConfig,
            ),
            connectionState = dispatcher.getConnectionState(),
        )
    }
}