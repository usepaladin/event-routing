package paladin.router.services.listener

import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.listener.KafkaMessageListenerContainer
import org.springframework.stereotype.Service
import paladin.router.pojo.listener.EventListener
import java.util.concurrent.ConcurrentHashMap

@Service
class EventListenerRegistry(
    private val kafkaConsumerFactory: DefaultKafkaConsumerFactory<Any, Any>
) {
    private val listeners = ConcurrentHashMap<String, EventListener>()
    private val activeContainers = ConcurrentHashMap<String, KafkaMessageListenerContainer<*,*>>()

    @Throws(IllegalArgumentException::class)
    fun registerListener(listener: EventListener) {
        if (listeners.containsKey(listener.topic)) {
            throw IllegalArgumentException("Listener for topic ${listener.topic} already registered")
        }

        listeners[listener.topic] = listener
    }

    fun unregisterListener(listener: EventListener) {

    }

    fun getListener(topic: String): EventListener? {
        return listeners[topic]
    }
}