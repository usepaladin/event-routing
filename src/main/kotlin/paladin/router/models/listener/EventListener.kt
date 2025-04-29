package paladin.router.models.listener

import org.apache.kafka.clients.consumer.ConsumerRecord
import paladin.router.enums.configuration.Broker
import paladin.router.models.dispatch.MessageDispatcher
import paladin.router.services.dispatch.DispatchService

data class EventListener(
    val topic: String,
    val runOnStartup: Boolean = false,
    val groupId: String,
    val key: Broker.BrokerFormat,
    val value: Broker.BrokerFormat,
    val dispatchers: List<MessageDispatcher> = emptyList(),
    private val dispatchService: DispatchService
) {
    fun start() {}
    fun stop() {}
    fun processMessage(message: ConsumerRecord<Any, Any>) {}
    fun build() {}
}
