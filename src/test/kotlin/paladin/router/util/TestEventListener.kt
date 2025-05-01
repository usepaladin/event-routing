package paladin.router.util

import org.apache.kafka.clients.consumer.ConsumerRecord
import paladin.router.enums.configuration.Broker
import paladin.router.models.listener.EventListener
import paladin.router.services.dispatch.DispatchService
import java.util.*
import java.util.concurrent.CopyOnWriteArrayList

class TestEventListener(
    id: UUID? = null,
    topic: String,
    runOnStartup: Boolean = false,
    groupId: String,
    key: Broker.BrokerFormat,
    value: Broker.BrokerFormat,
    dispatchService: DispatchService
) : EventListener(id, topic, runOnStartup, groupId, key, value, dispatchService) {
    val receivedMessages = CopyOnWriteArrayList<Any>()

    override fun processMessage(message: ConsumerRecord<Any, Any>) {
        super.processMessage(message)
        receivedMessages.add(message.value())
    }
}