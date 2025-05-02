//package paladin.router.services.listener
//
//import io.github.oshai.kotlinlogging.KLogger
//import org.apache.kafka.clients.consumer.ConsumerRecord
//import org.junit.jupiter.api.AfterEach
//import org.junit.jupiter.api.Assertions.*
//import org.junit.jupiter.api.BeforeEach
//import org.junit.jupiter.api.Test
//import org.junit.jupiter.api.extension.ExtendWith
//import org.mockito.ArgumentMatchers.any
//import org.mockito.Mock
//import org.mockito.Mockito.*
//import org.mockito.junit.jupiter.MockitoExtension
//import org.springframework.boot.ApplicationArguments
//import org.springframework.kafka.core.DefaultKafkaConsumerFactory
//import org.springframework.kafka.listener.KafkaMessageListenerContainer
//import org.springframework.kafka.listener.MessageListener
//import paladin.router.enums.configuration.Broker
//import paladin.router.exceptions.ActiveListenerException
//import paladin.router.exceptions.ListenerNotFoundException
//import paladin.router.models.listener.EventListener
//import paladin.router.models.listener.ListenerRegistrationRequest
//import paladin.router.repository.EventListenerRepository
//import paladin.router.services.dispatch.DispatchService
//import java.io.IOException
//import java.util.*
//import java.util.concurrent.ConcurrentHashMap
//
//@ExtendWith(MockitoExtension::class)
//class EventListenerRegistryTest {
//
//    @Mock
//    private lateinit var kafkaConsumerFactory: DefaultKafkaConsumerFactory<Any, Any>
//
//    @Mock
//    private lateinit var eventListenerRepository: EventListenerRepository
//
//    @Mock
//    private lateinit var dispatchService: DispatchService
//
//    @Mock
//    private lateinit var logger: KLogger
//
//    @Mock
//    private lateinit var args: ApplicationArguments
//
//    @Mock
//    private lateinit var listenerContainer: KafkaMessageListenerContainer<Any, Any>
//
//    private lateinit var registry: EventListenerRegistry
//
//    private val testTopic = "test-topic"
//    private val testGroupId = "test-group"
//
//    @BeforeEach
//    fun setUp() {
//        registry = EventListenerRegistry(
//            kafkaConsumerFactory,
//            eventListenerRepository,
//            dispatchService,
//            logger
//        )
//
//        // Setup common mocks
//        `when`(eventListenerRepository.save(any())).thenAnswer {
//            val entity = it.arguments[0] as paladin.router.entities.EventListenerEntity
//            entity.id = UUID.randomUUID()
//            entity
//        }
//    }
//
//    @AfterEach
//    fun tearDown() {
//        // Stop any running containers that might have been started
//        try {
//            val field = EventListenerRegistry::class.java.getDeclaredField("activeContainers")
//            field.isAccessible = true
//            val activeContainers = field.get(registry) as ConcurrentHashMap<String, KafkaMessageListenerContainer<*, *>>
//
//            activeContainers.forEach { (_, container) ->
//                if (container.isRunning) {
//                    container.stop()
//                }
//            }
//        } catch (e: Exception) {
//            // Ignore exceptions during cleanup
//        }
//    }
//
//    @Test
//    fun `test register listener successfully`() {
//        // Arrange
//        val request = createTestListenerRequest()
//
//        // Act
//        val result = registry.registerListener(request)
//
//        // Assert
//        verify(eventListenerRepository).save(any())
//        assertNotNull(result.id)
//        assertEquals(testTopic, result.topic)
//        assertEquals(testGroupId, result.groupId)
//    }
//
//    @Test
//    fun `test register listener with existing topic throws exception`() {
//        // Arrange
//        val request = createTestListenerRequest()
//        registry.registerListener(request)
//
//        // Act & Assert
//        assertThrows(IllegalArgumentException::class.java) {
//            registry.registerListener(request)
//        }
//    }
//
//    @Test
//    fun `test get listener returns registered listener`() {
//        // Arrange
//        val request = createTestListenerRequest()
//        registry.registerListener(request)
//
//        // Act
//        val result = registry.getListener(testTopic)
//
//        // Assert
//        assertNotNull(result)
//        assertEquals(testTopic, result?.topic)
//    }
//
//    @Test
//    fun `test get all topic listeners returns all registered listeners`() {
//        // Arrange
//        val request1 = createTestListenerRequest()
//        val request2 = createTestListenerRequest("another-topic", "another-group")
//        registry.registerListener(request1)
//        registry.registerListener(request2)
//
//        // Act
//        val results = registry.getAllTopicListeners()
//
//        // Assert
//        assertEquals(2, results.size)
//        assertTrue(results.any { it.topic == testTopic })
//        assertTrue(results.any { it.topic == "another-topic" })
//    }
//
//    @Test
//    fun `test unregister listener removes it from registry`() {
//        // Arrange
//        val request = createTestListenerRequest()
//        registry.registerListener(request)
//
//        // Act
//        registry.unregisterListener(testTopic)
//
//        // Assert
//        val result = registry.getListener(testTopic)
//        assertNull(result)
//        verify(dispatchService).removeSourceTopic(testTopic)
//    }
//
//    @Test
//    fun `test unregister active listener throws exception`() {
//        // Arrange
//        val request = createTestListenerRequest()
//        registry.registerListener(request)
//
//        // Mock active container
//        mockActiveContainer(testTopic, true)
//
//        // Act & Assert
//        assertThrows(ActiveListenerException::class.java) {
//            registry.unregisterListener(testTopic)
//        }
//    }
//
//    @Test
//    fun `test edit listener successfully updates properties`() {
//        // Arrange
//        val originalRequest = createTestListenerRequest()
//        registry.registerListener(originalRequest)
//
//        val updatedRequest = createTestListenerRequest(
//            testTopic,
//            "updated-group",
//            Broker.BrokerFormat.AVRO,
//            Broker.BrokerFormat.JSON
//        )
//
//        // Act
//        val result = registry.editListener(updatedRequest)
//
//        // Assert
//        assertEquals("updated-group", result.groupId)
//        assertEquals(Broker.BrokerFormat.AVRO, result.key)
//        assertEquals(Broker.BrokerFormat.JSON, result.value)
//        verify(eventListenerRepository, times(2)).save(any())
//    }
//
//    @Test
//    fun `test edit non-existent listener throws exception`() {
//        // Arrange
//        val request = createTestListenerRequest()
//
//        // Act & Assert
//        assertThrows(ListenerNotFoundException::class.java) {
//            registry.editListener(request)
//        }
//    }
//
//    @Test
//    fun `test edit active listener throws exception`() {
//        // Arrange
//        val request = createTestListenerRequest()
//        registry.registerListener(request)
//
//        // Mock active container
//        mockActiveContainer(testTopic, true)
//
//        // Act & Assert
//        assertThrows(ActiveListenerException::class.java) {
//            registry.editListener(request)
//        }
//    }
//
//    @Test
//    fun `test start listener creates and starts container`() {
//        // Arrange
//        val request = createTestListenerRequest()
//        registry.registerListener(request)
//
//        // Mock container creation and start
//        mockContainerCreation()
//
//        // Act
//        registry.startListener(testTopic)
//
//        // Assert
//        verify(listenerContainer).start()
//        assertTrue(getActiveContainers().containsKey(testTopic))
//    }
//
//    @Test
//    fun `test start listener with non-existent topic throws exception`() {
//        // Act & Assert
//        assertThrows(ListenerNotFoundException::class.java) {
//            registry.startListener("non-existent-topic")
//        }
//    }
//
//    @Test
//    fun `test start already started listener throws exception`() {
//        // Arrange
//        val request = createTestListenerRequest()
//        registry.registerListener(request)
//
//        // Mock container creation and start
//        mockContainerCreation()
//        registry.startListener(testTopic)
//
//        // Act & Assert
//        assertThrows(IllegalArgumentException::class.java) {
//            registry.startListener(testTopic)
//        }
//    }
//
//    @Test
//    fun `test stop listener stops and removes container`() {
//        // Arrange
//        val request = createTestListenerRequest()
//        registry.registerListener(request)
//
//        // Mock container creation, start and stop
//        mockContainerCreation()
//        registry.startListener(testTopic)
//        `when`(listenerContainer.isRunning).thenReturn(true)
//
//        // Act
//        registry.stopListener(testTopic)
//
//        // Assert
//        verify(listenerContainer).stop()
//        assertFalse(getActiveContainers().containsKey(testTopic))
//    }
//
//    @Test
//    fun `test stop listener with non-running container throws exception`() {
//        // Arrange
//        val request = createTestListenerRequest()
//        registry.registerListener(request)
//
//        // Act & Assert
//        assertThrows(IllegalArgumentException::class.java) {
//            registry.stopListener(testTopic)
//        }
//    }
//
//    @Test
//    fun `test stop listener handles IOException during stop`() {
//        // Arrange
//        val request = createTestListenerRequest()
//        registry.registerListener(request)
//
//        // Mock container creation and start
//        mockContainerCreation()
//        registry.startListener(testTopic)
//
//        // Mock container stop throwing exception
//        `when`(listenerContainer.isRunning).thenReturn(true)
//        doThrow(IOException("Test exception")).`when`(listenerContainer).stop()
//
//        // Act & Assert
//        assertThrows(IllegalArgumentException::class.java) {
//            registry.stopListener(testTopic)
//        }
//
//        // Verify container was removed despite exception
//        assertFalse(getActiveContainers().containsKey(testTopic))
//    }
//
//    @Test
//    fun `test message processing is delegated to event listener`() {
//        // Arrange
//        val request = createTestListenerRequest()
//        val listener = registry.registerListener(request)
//
//        // Create a spy on the EventListener to verify processMessage is called
//        val listenerSpy = spy(listener)
//
//        // Replace the original listener with our spy using reflection
//        val listenersField = EventListenerRegistry::class.java.getDeclaredField("listeners")
//        listenersField.isAccessible = true
//        val listeners = listenersField.get(registry) as ConcurrentHashMap<String, EventListener>
//        listeners[testTopic] = listenerSpy
//
//        // Mock container creation to capture the MessageListener
//        mockContainerCreation(true)
//
//        // Start the listener to register the MessageListener
//        registry.startListener(testTopic)
//
//        // Create a test record
//        val record = mock(ConsumerRecord::class.java) as ConsumerRecord<Any, Any>
//
//        // Act - Get the MessageListener and invoke it with our record
//        val containerPropsCaptor = argumentCaptor<org.springframework.kafka.listener.ContainerProperties>()
//        verify(kafkaConsumerFactory).createContainer(containerPropsCaptor.capture())
//        val messageListener = containerPropsCaptor.value.messageListener as MessageListener<Any, Any>
//        messageListener.onMessage(record)
//
//        // Assert
//        verify(listenerSpy).processMessage(record)
//    }
//
//    // Helper methods
//
//    private fun createTestListenerRequest(
//        topic: String = testTopic,
//        groupId: String = testGroupId,
//        key: Broker.BrokerFormat = Broker.BrokerFormat.STRING,
//        value: Broker.BrokerFormat = Broker.BrokerFormat.STRING
//    ): ListenerRegistrationRequest {
//        return ListenerRegistrationRequest(
//            topic = topic,
//            groupId = groupId,
//            key = key,
//            value = value
//        )
//    }
//
//    private fun mockActiveContainer(topic: String, isRunning: Boolean) {
//        val activeContainersField = EventListenerRegistry::class.java.getDeclaredField("activeContainers")
//        activeContainersField.isAccessible = true
//        val activeContainers =
//            activeContainersField.get(registry) as ConcurrentHashMap<String, KafkaMessageListenerContainer<*, *>>
//
//        val container = mock(KafkaMessageListenerContainer::class.java)
//        `when`(container.isRunning).thenReturn(isRunning)
//        activeContainers[topic] = container
//    }
//
//    private fun mockContainerCreation(captureContainerProps: Boolean = false) {
//        if (captureContainerProps) {
//            `when`(kafkaConsumerFactory.createContainer(any())).thenReturn(listenerContainer)
//        } else {
//            // Use reflection to set up the active container directly
//            val container = mock(KafkaMessageListenerContainer::class.java)
//            val activeContainersField = EventListenerRegistry::class.java.getDeclaredField("activeContainers")
//            activeContainersField.isAccessible = true
//            val activeContainers =
//                activeContainersField.get(registry) as ConcurrentHashMap<String, KafkaMessageListenerContainer<*, *>>
//            activeContainers[testTopic] = container
//        }
//    }
//
//    private fun getActiveContainers(): ConcurrentHashMap<String, KafkaMessageListenerContainer<*, *>> {
//        val field = EventListenerRegistry::class.java.getDeclaredField("activeContainers")
//        field.isAccessible = true
//        return field.get(registry) as ConcurrentHashMap<String, KafkaMessageListenerContainer<*, *>>
//    }
//
//    // Mock extensions for Mockito argument capture
//    private fun <T> argumentCaptor(): org.mockito.ArgumentCaptor<T> {
//        return org.mockito.ArgumentCaptor.forClass(java.lang.Object::class.java) as org.mockito.ArgumentCaptor<T>
//    }
//}