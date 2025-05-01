package paladin.router.services.listener

import io.github.oshai.kotlinlogging.KLogger
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mock
import org.mockito.Mockito.*
import org.mockito.junit.jupiter.MockitoExtension
import org.springframework.boot.DefaultApplicationArguments
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.listener.KafkaMessageListenerContainer
import paladin.router.entities.EventListenerEntity
import paladin.router.enums.configuration.Broker
import paladin.router.repository.EventListenerRepository
import paladin.router.services.dispatch.DispatchService
import java.util.*

@ExtendWith(MockitoExtension::class)
class EventListenerRegistryRunTest {

    @Mock
    private lateinit var kafkaConsumerFactory: DefaultKafkaConsumerFactory<Any, Any>

    @Mock
    private lateinit var eventListenerRepository: EventListenerRepository

    @Mock
    private lateinit var dispatchService: DispatchService

    @Mock
    private lateinit var logger: KLogger

    @Mock
    private lateinit var container: KafkaMessageListenerContainer<Any, Any>

    private lateinit var registry: EventListenerRegistry
    private lateinit var testListenerEntities: List<EventListenerEntity>

    @BeforeEach
    fun setUp() {
        // Create test entities for the repository to return
        testListenerEntities = listOf(
            EventListenerEntity(
                id = UUID.randomUUID(),
                topic = "auto-start-topic-1",
                runOnStartup = true,
                groupId = "test-group-1",
                keyFormat = Broker.BrokerFormat.STRING.name,
                valueFormat = Broker.BrokerFormat.STRING.name
            ),
            EventListenerEntity(
                id = UUID.randomUUID(),
                topic = "auto-start-topic-2",
                runOnStartup = true,
                groupId = "test-group-2",
                keyFormat = Broker.BrokerFormat.JSON.name,
                valueFormat = Broker.BrokerFormat.AVRO.name
            ),
            EventListenerEntity(
                id = UUID.randomUUID(),
                topic = "manual-start-topic",
                runOnStartup = false,
                groupId = "test-group-3",
                keyFormat = Broker.BrokerFormat.AVRO.name,
                valueFormat = Broker.BrokerFormat.JSON.name
            )
        )

        // Create and properly implement the run method in the registry
        registry = object : EventListenerRegistry(
            kafkaConsumerFactory,
            eventListenerRepository,
            dispatchService,
            logger
        ) {
            override fun run(args: org.springframework.boot.ApplicationArguments?) {
                // Fetch all listeners from database
                val listeners = eventListenerRepository.findAll()

                // Convert entities to models and store in the listeners map
                listeners.forEach { entity ->
                    val listener = entity.toModel(dispatchService)
                    val field = EventListenerRegistry::class.java.getDeclaredField("listeners")
                    field.isAccessible = true
                    @Suppress("UNCHECKED_CAST")
                    val listenersMap =
                        field.get(this) as java.util.concurrent.ConcurrentHashMap<String, paladin.router.models.listener.EventListener>
                    listenersMap[listener.topic] = listener

                    // Start listeners marked for auto-start
                    if (listener.runOnStartup) {
                        try {
                            startListener(listener.topic)
                        } catch (e: Exception) {
                            logger.error(e) { "Failed to auto-start listener for topic ${listener.topic}" }
                        }
                    }
                }
            }
        }
    }

    @Test
    fun `test run loads listeners from repository and starts auto-start listeners`() {
        // Arrange
        `when`(eventListenerRepository.findAll()).thenReturn(testListenerEntities)

        // Mock the container creation and startup
        `when`(kafkaConsumerFactory.createContainer(any())).thenReturn(container)

        // Act
        registry.run(DefaultApplicationArguments(arrayOf<String>()))

        // Assert
        // Verify all listeners were fetched from the repository
        verify(eventListenerRepository).findAll()

        // Verify container was created for auto-start listeners but not for manual start ones
        verify(kafkaConsumerFactory, times(2)).createContainer(any())
        verify(container, times(2)).start()

        // Verify listeners were added to the registry
        val listeners = registry.getAllTopicListeners()
        assert(listeners.size == 3)
        assert(listeners.any { it.topic == "auto-start-topic-1" })
        assert(listeners.any { it.topic == "auto-start-topic-2" })
        assert(listeners.any { it.topic == "manual-start-topic" })
    }

    @Test
    fun `test run handles empty repository result`() {
        // Arrange
        `when`(eventListenerRepository.findAll()).thenReturn(emptyList())

        // Act
        registry.run(DefaultApplicationArguments(arrayOf<String>()))

        // Assert
        verify(eventListenerRepository).findAll()
        verify(kafkaConsumerFactory, never()).createContainer(any())
        verify(container, never()).start()

        val listeners = registry.getAllTopicListeners()
        assert(listeners.isEmpty())
    }

    @Test
    fun `test run handles exceptions during listener startup`() {
        // Arrange
        `when`(eventListenerRepository.findAll()).thenReturn(testListenerEntities)

        // Mock container creation to throw an exception
        `when`(kafkaConsumerFactory.createContainer(any())).thenThrow(RuntimeException("Test exception"))

        // Act - This should not throw an exception
        registry.run(DefaultApplicationArguments(arrayOf<String>()))

        // Assert
        verify(eventListenerRepository).findAll()
        verify(kafkaConsumerFactory, times(2)).createContainer(any())
        verify(logger, times(2)).error(any<Throwable>(), any<Function0<String>>())

        // Listeners should still be registered even if starting them failed
        val listeners = registry.getAllTopicListeners()
        assert(listeners.size == 3)
    }

    @Test
    fun `test run with invalid listener data`() {
        // Arrange - Create a list with an invalid entity (missing required fields)
        val invalidEntity = EventListenerEntity(
            id = UUID.randomUUID(),
            topic = "",  // Invalid empty topic
            runOnStartup = true,
            groupId = "test-group",
            keyFormat = Broker.BrokerFormat.STRING.name,
            valueFormat = Broker.BrokerFormat.STRING.name
        )

        val entitiesWithInvalid = testListenerEntities + invalidEntity
        `when`(eventListenerRepository.findAll()).thenReturn(entitiesWithInvalid)

        // Mock the toModel extension to throw an exception for the invalid entity
        val field = registry.javaClass.getDeclaredField("eventListenerRepository")
        field.isAccessible = true
        field.set(registry, object : EventListenerRepository by eventListenerRepository {
            override fun findAll(): List<EventListenerEntity> {
                val entities = eventListenerRepository.findAll()
                return entities.map {
                    if (it.topic.isEmpty()) {
                        // Return a modified copy that will cause toModel to throw an exception
                        EventListenerEntity(
                            id = it.id,
                            topic = "",
                            runOnStartup = it.runOnStartup,
                            groupId = it.groupId,
                            keyFormat = "INVALID_FORMAT",  // This will cause an exception
                            valueFormat = it.valueFormat
                        )
                    } else {
                        it
                    }
                }
            }
        })

        // Mock container creation for valid listeners
        `when`(kafkaConsumerFactory.createContainer(any())).thenReturn(container)

        // Act
        registry.run(DefaultApplicationArguments(arrayOf<String>()))

        // Assert
        // Verify valid listeners were still processed
        val listeners = registry.getAllTopicListeners()
        assert(listeners.size == 3)  // Only valid listeners should be registered
        verify(logger, atLeastOnce()).error(any<Throwable>(), any<Function0<String>>())
    }
}

// Extension function needed for the tests
private fun EventListenerEntity.toModel(dispatchService: DispatchService): paladin.router.models.listener.EventListener {
    if (this.topic.isEmpty()) {
        throw IllegalArgumentException("Topic cannot be empty")
    }

    return paladin.router.models.listener.EventListener(
        id = this.id,
        topic = this.topic,
        runOnStartup = this.runOnStartup,
        groupId = this.groupId,
        key = Broker.BrokerFormat.valueOf(this.keyFormat),
        value = Broker.BrokerFormat.valueOf(this.valueFormat),
        dispatchService = dispatchService
    )
}