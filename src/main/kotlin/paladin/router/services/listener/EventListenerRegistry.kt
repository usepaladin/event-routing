package paladin.router.services.listener

import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.listener.KafkaMessageListenerContainer
import org.springframework.stereotype.Service
import paladin.router.models.listener.EventListener
import paladin.router.models.listener.ListenerRegistrationRequest
import paladin.router.services.dispatch.DispatchService
import java.util.concurrent.ConcurrentHashMap

@Service
class EventListenerRegistry(
    private val kafkaConsumerFactory: DefaultKafkaConsumerFactory<Any, Any>,
    private val dispatchService: DispatchService
) {
    private val listeners = ConcurrentHashMap<String, EventListener>()
    private val activeContainers = ConcurrentHashMap<String, KafkaMessageListenerContainer<*,*>>()

    @Throws(IllegalArgumentException::class)
    fun registerListener(listener: ListenerRegistrationRequest): EventListener {
        if (listeners.containsKey(listener.topic)) {
            throw IllegalArgumentException("Listener for topic ${listener.topic} already registered")
        }

        TODO()
    }

    fun editListener(listener: ListenerRegistrationRequest): EventListener {
        TODO()
    }

    fun unregisterListener(listener: EventListener) {

    }

    fun getListener(topic: String): EventListener? {
        return listeners[topic]
    }

    fun getAllTopicListeners(): List<EventListener> {
        return listeners.values.toList()
    }

    fun startListener(topic: String) {
        TODO()
    }

    fun pauseListener(topic: String) {
        TODO()
    }
}