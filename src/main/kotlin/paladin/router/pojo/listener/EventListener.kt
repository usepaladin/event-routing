package paladin.router.pojo.listener

import paladin.router.pojo.dispatch.MessageDispatcher
import paladin.router.services.dispatch.DispatchService

data class EventListener(
    val topic: String,
    val groupId: String,
    val key: PayloadFormat,
    val value: PayloadFormat,
    val dispatchers: List<MessageDispatcher> = emptyList(),
    private val dispatchService: DispatchService
){
    fun start(){}
    fun stop(){}
    fun processMessage(){}
    fun build(){}
}
