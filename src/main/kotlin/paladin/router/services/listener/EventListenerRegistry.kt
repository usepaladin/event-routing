package paladin.router.services.listener

import io.github.oshai.kotlinlogging.KLogger
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.listener.KafkaMessageListenerContainer
import org.springframework.kafka.listener.MessageListener
import org.springframework.stereotype.Service
import paladin.router.exceptions.ActiveListenerException
import paladin.router.exceptions.ListenerNotFoundException
import paladin.router.models.listener.EventListener
import paladin.router.models.listener.ListenerRegistrationRequest
import paladin.router.repository.EventListenerRepository
import paladin.router.services.dispatch.DispatchService
import paladin.router.util.factory.ConsumerConfigFactory
import paladin.router.util.factory.EntityFactory.toEntity
import java.io.IOException
import java.util.concurrent.ConcurrentHashMap

@Service
class EventListenerRegistry(
    private val kafkaConsumerFactory: DefaultKafkaConsumerFactory<Any, Any>,
    private val eventListenerRepository: EventListenerRepository,
    private val dispatchService: DispatchService,
    private val logger: KLogger
) : ApplicationRunner {
    private val listeners = ConcurrentHashMap<String, EventListener>()
    private val activeContainers = ConcurrentHashMap<String, KafkaMessageListenerContainer<*, *>>()

    // Fetch all listeners from the database or configuration and start up all default listeners to consume topics
    override fun run(args: ApplicationArguments?) {
        logger.info { "Initializing EventListenerRegistry and starting default listeners" }

        try {
            // Fetch all listeners from the database
            val storedListeners = eventListenerRepository.findAll()

            if (storedListeners.isEmpty()) {
                logger.info { "No listeners found in the database" }
                return
            }

            // Convert to EventListener objects and add to the listeners map
            storedListeners.forEach { entity ->
                try {
                    val listener = EventListener(
                        id = entity.id,
                        topic = entity.topic,
                        runOnStartup = entity.runOnStartup,
                        groupId = entity.groupId,
                        key = entity.keyFormat,
                        value = entity.valueFormat,
                        config = entity.consumerProperties,
                        dispatchService = dispatchService
                    )

                    listeners[entity.topic] = listener
                    logger.info { "Loaded listener for topic ${entity.topic} from database" }

                    // Start listeners that are configured to run on startup
                    if (entity.runOnStartup) {
                        try {
                            startListener(entity.topic)
                            logger.info { "Started listener for topic ${entity.topic} on startup" }
                        } catch (e: Exception) {
                            logger.error(e) { "Failed to start listener for topic ${entity.topic} on startup" }
                        }
                    }
                } catch (e: Exception) {
                    logger.error(e) { "Failed to initialize listener for topic ${entity.topic}" }
                }
            }
        } catch (e: Exception) {
            logger.error(e) { "Failed to initialize EventListenerRegistry" }
        }
    }

    @Throws(IllegalArgumentException::class)
    fun registerListener(listener: ListenerRegistrationRequest): EventListener {
        if (listeners.containsKey(listener.topic)) {
            throw IllegalArgumentException("Listener for topic ${listener.topic} already registered")
        }

        return buildListener(listener).also {
            listeners[listener.topic] = it
        }
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

            return buildListener(updatedListener, it).also { listener ->
                listeners[updatedListener.topic] = listener
            }
        }
    }

    /**
     * Creates an event listener and performs the following actions:
     *  - Builds Record consumer
     *  - Validates consumer configuration
     *  - Saves the listener to the database
     */
    private fun buildListener(request: ListenerRegistrationRequest, prev: EventListener? = null): EventListener {
        val listener: EventListener = prev.let {
            if (it == null) {
                return@let EventListener(
                    topic = request.topic,
                    groupId = request.groupId,
                    key = request.key,
                    value = request.value,
                    dispatchService = dispatchService,
                    config = request.config,
                    runOnStartup = request.runOnStartup
                )
            }

            it.apply {
                this.topic = request.topic
                this.groupId = request.groupId
                this.key = request.key
                this.value = request.value
            }
        }

        return saveListener(listener)
    }

    private fun saveListener(listener: EventListener): EventListener {
        eventListenerRepository.save(listener.toEntity()).also {
            it.id.let { id ->
                if (id == null) {
                    throw IllegalArgumentException("Listener for topic ${listener.topic} could not be saved")
                }
            }

            listener.id = it.id
            logger.info { "Listener for topic ${listener.topic} registered successfully" }
        }

        return listener
    }

    fun unregisterListener(topic: String) {
        activeContainers[topic].let {
            if (it != null && it.isRunning) {
                throw ActiveListenerException("Listener for topic $topic must be stopped before removal")
            }
        }

        listeners.remove(topic).also {
            dispatchService.removeSourceTopic(topic)
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

        // Create a consumer factory with the appropriate deserializers based on listener configuration
        val consumerFactory = createConsumerFactory(listener)
        val containerProps = ContainerProperties(topic)

        containerProps.messageListener = MessageListener {
            logger.debug { "Event Listener => Topic: $topic => Event consumed" }
            listener.processMessage(it)
        }

        val container = KafkaMessageListenerContainer(consumerFactory, containerProps)
        container.start()

        activeContainers[topic] = container
        logger.info { "Listener for topic $topic started successfully with key format ${listener.key} and value format ${listener.value}" }
    }

    /**
     * Creates a consumer factory with the appropriate deserializers based on the listener configuration
     */
    private fun createConsumerFactory(listener: EventListener): DefaultKafkaConsumerFactory<*, *> {
        // Get base configuration from the existing consumer factory
        val baseConfig = HashMap(kafkaConsumerFactory.configurationProperties)
        val (config, keyDeserializer, valueDeserializer) = ConsumerConfigFactory.buildConsumer(baseConfig, listener)

        // Create a new consumer factory with the updated configuration and deserializers
        return DefaultKafkaConsumerFactory(config, keyDeserializer, valueDeserializer)
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
    }
}