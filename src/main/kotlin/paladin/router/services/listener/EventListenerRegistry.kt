package paladin.router.services.listener

import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.listener.KafkaMessageListenerContainer
import org.springframework.kafka.listener.MessageListener
import org.springframework.stereotype.Service
import paladin.router.exceptions.ActiveListenerException
import paladin.router.exceptions.ListenerNotFoundException
import paladin.router.models.dispatch.MessageDispatcher
import paladin.router.models.listener.EventListener
import paladin.router.models.listener.ListenerRegistrationRequest
import paladin.router.services.dispatch.DispatchService
import java.io.IOException
import java.util.concurrent.ConcurrentHashMap

@Service
class EventListenerRegistry(
    private val kafkaConsumerFactory: DefaultKafkaConsumerFactory<Any, Any>,
    private val dispatchService: DispatchService
) {
    private val listeners = ConcurrentHashMap<String, EventListener>()
    private val activeContainers = ConcurrentHashMap<String, KafkaMessageListenerContainer<*, *>>()

    @Throws(IllegalArgumentException::class)
    fun registerListener(listener: ListenerRegistrationRequest): EventListener {
        if (listeners.containsKey(listener.topic)) {
            throw IllegalArgumentException("Listener for topic ${listener.topic} already registered")
        }

        return addListener(listener)
    }

    fun editListener(updatedListener: ListenerRegistrationRequest): EventListener {
        listeners[updatedListener.topic].let {
            if (it == null) {
                throw ListenerNotFoundException("Listener for topic ${updatedListener.topic} not found")
            }

            activeContainers[updatedListener.topic]?.let { container ->
                if (container.isRunning) {
                    throw ActiveListenerException("Listener for topic ${updatedListener.topic} must be stopped before editing")
                }
            }

            return addListener(updatedListener)
        }
    }

    private fun addListener(listener: ListenerRegistrationRequest): EventListener {
        val dispatchers: List<MessageDispatcher> = listener.brokers.map {
            dispatchService.getDispatcher(it)
                ?: throw IllegalArgumentException("Dispatcher for broker $it not found")
        }

        return EventListener(
            topic = listener.topic,
            groupId = listener.groupId,
            key = listener.key,
            value = listener.value,
            dispatchers = dispatchers,
            dispatchService = dispatchService
        ).also { eventListener ->
            listeners[listener.topic] = eventListener
        }
    }

    fun unregisterListener(topic: String) {
        activeContainers[topic].let {
            if (it != null && it.isRunning) {
                throw ActiveListenerException("Listener for topic $topic must be stopped before removal")
            }
        }

        listeners[topic]?.stop().also {
            listeners.remove(topic)
        }
    }

    fun getListener(topic: String): EventListener? {
        return listeners[topic]
    }

    fun getAllTopicListeners(): List<EventListener> {
        return listeners.values.toList()
    }

    @Throws(IllegalArgumentException::class)
    fun startListener(topic: String) {
        val listener: EventListener = listeners[topic]
            ?: throw ListenerNotFoundException("Listener for topic $topic not found")

        if (activeContainers.containsKey(topic)) {
            throw IllegalArgumentException("Listener for topic $topic already started")
        }

        val containerProps = ContainerProperties(topic)
        containerProps.messageListener = MessageListener {
            listener.processMessage(it)
        }

        val container = KafkaMessageListenerContainer(kafkaConsumerFactory, containerProps)
        container.start()

        activeContainers[topic] = container
        listener.start()
    }

    @Throws(IllegalArgumentException::class)
    fun stopListener(topic: String) {
        activeContainers[topic].let {
            if (it == null || !it.isRunning) {
                throw IllegalArgumentException("Listener for topic $topic not currently run")
            }

            try {
                it.stop()
            } catch (e: IOException) {
                throw IllegalArgumentException("Failed to stop listener for topic $topic", e)
            } finally {
                activeContainers.remove(topic)
            }
        }

        listeners[topic]?.stop()
    }
}