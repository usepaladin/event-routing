package paladin.router.models.listener

import org.apache.kafka.clients.consumer.ConsumerRecord
import paladin.router.enums.configuration.Broker
import paladin.router.services.dispatch.DispatchService
import java.util.*

data class EventListener(
    var id: UUID? = null,
    var topic: String,
    var runOnStartup: Boolean = false,
    var groupId: String,
    var key: Broker.ProducerFormat,
    var value: Broker.ProducerFormat,
    val config: AdditionalConsumerProperties,
    private val dispatchService: DispatchService
) {
    fun processMessage(message: ConsumerRecord<Any, Any>) {
        dispatchService.dispatchEvents(message.key(), message.value(), this.topic)
    }
}
