package paladin.router.models.listener

import org.apache.kafka.clients.consumer.ConsumerRecord
import paladin.router.enums.configuration.Broker
import paladin.router.services.dispatch.DispatchService
import java.util.*

open class EventListener(
    var id: UUID? = null,
    var topic: String,
    var runOnStartup: Boolean = false,
    var groupId: String,
    var key: Broker.BrokerFormat,
    var value: Broker.BrokerFormat,
    private val dispatchService: DispatchService
) {
    open fun processMessage(message: ConsumerRecord<Any, Any>) {
        dispatchService.dispatchEvents(message.key(), message.value(), this)
    }
}
