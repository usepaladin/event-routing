package paladin.router.services.listener

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.ArgumentCaptor
import org.mockito.Mockito
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.kafka.test.utils.KafkaTestUtils
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import paladin.router.enums.configuration.Broker
import paladin.router.models.listener.ListenerRegistrationRequest
import paladin.router.repository.EventListenerRepository
import paladin.router.services.dispatch.DispatchService
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

@SpringBootTest
@DirtiesContext
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = ["test-topic"])
class EventListenerRegistryIT {

    @Autowired
    private lateinit var embeddedKafka: EmbeddedKafkaBroker

    @MockBean
    private lateinit var eventListenerRepository: EventListenerRepository

    @MockBean
    private lateinit var dispatchService: DispatchService

    private lateinit var registry: EventListenerRegistry
    private lateinit var kafkaConsumerFactory: DefaultKafkaConsumerFactory<Any, Any>
    private lateinit var kafkaProducer: KafkaProducer<String, String>

    private val logger = KotlinLogging.logger {}
    private val testTopic = "test-topic"
    private val testKey = "test-key"
    private val testValue = "test-value"
    private val testGroupId = "test-group"

    @BeforeEach
    fun setUp() {
        // Set up Kafka consumer factory
        val consumerProps = KafkaTestUtils.consumerProps(
            testGroupId,
            "true",
            embeddedKafka
        )
        consumerProps[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        consumerProps[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java

        kafkaConsumerFactory = DefaultKafkaConsumerFactory(consumerProps)

        // Set up Kafka producer
        val producerProps = KafkaTestUtils.producerProps(embeddedKafka)
        producerProps[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        producerProps[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java

        kafkaProducer = KafkaProducer(producerProps)

        // Configure event listener repository mock to return the entity with an ID
        Mockito.`when`(eventListenerRepository.save(Mockito.any())).thenAnswer {
            val entity = it.arguments[0] as paladin.router.entities.EventListenerEntity
            entity.id = java.util.UUID.randomUUID()
            entity
        }

        // Initialize the event listener registry
        registry = EventListenerRegistry(
            kafkaConsumerFactory,
            eventListenerRepository,
            dispatchService,
            logger
        )
    }

    @AfterEach
    fun tearDown() {
        // Make sure to stop any active listeners
        try {
            registry.stopListener(testTopic)
        } catch (e: Exception) {
            // Ignore exceptions during cleanup
        }

        // Close producer
        kafkaProducer.close()
    }

    @Test
    fun `test listener receives and processes Kafka messages`() {
        // Arrange - Register and start a listener
        val request = ListenerRegistrationRequest(
            topic = testTopic,
            groupId = testGroupId,
            key = Broker.BrokerFormat.STRING,
            value = Broker.BrokerFormat.STRING
        )

        registry.registerListener(request)
        registry.startListener(testTopic)

        // Use a latch to wait for the message to be processed
        val latch = CountDownLatch(1)

        // Configure the dispatch service mock to count down the latch
        Mockito.doAnswer {
            latch.countDown()
            null
        }.`when`(dispatchService).dispatchEvents(Mockito.any(), Mockito.any(), Mockito.any())

        // Act - Send a message to the topic
        val record = ProducerRecord(testTopic, testKey, testValue)
        kafkaProducer.send(record).get() // Wait for the send to complete

        // Wait for the message to be processed
        val messageProcessed = latch.await(10, TimeUnit.SECONDS)

        // Assert
        assertEquals(true, messageProcessed, "Message should have been processed")

        // Verify that the dispatcher was called with the correct message
        val keyCaptor = ArgumentCaptor.forClass(Any::class.java)
        val valueCaptor = ArgumentCaptor.forClass(Any::class.java)

        verify(dispatchService, times(1)).dispatchEvents(
            keyCaptor.capture(),
            valueCaptor.capture(),
            Mockito.any()
        )

        assertEquals(testKey, keyCaptor.value)
        assertEquals(testValue, valueCaptor.value)
    }

    @Test
    fun `test multiple listeners can process messages concurrently`() {
        // Arrange - Register and start two listeners on different topics
        val topic1 = "test-topic"
        val topic2 = "test-topic-2"

        // Create a new topic programmatically
        embeddedKafka.addTopics(topic2)

        val request1 = ListenerRegistrationRequest(
            topic = topic1,
            groupId = testGroupId,
            key = Broker.BrokerFormat.STRING,
            value = Broker.BrokerFormat.STRING
        )

        val request2 = ListenerRegistrationRequest(
            topic = topic2,
            groupId = testGroupId,
            key = Broker.BrokerFormat.STRING,
            value = Broker.BrokerFormat.STRING
        )

        registry.registerListener(request1)
        registry.registerListener(request2)
        registry.startListener(topic1)
        registry.startListener(topic2)

        // Use a latch to wait for both messages to be processed
        val latch = CountDownLatch(2)

        // Configure the dispatch service mock to count down the latch
        Mockito.doAnswer {
            latch.countDown()
            null
        }.`when`(dispatchService).dispatchEvents(Mockito.any(), Mockito.any(), Mockito.any())

        // Act - Send messages to both topics
        val record1 = ProducerRecord(topic1, testKey, testValue)
        val record2 = ProducerRecord(topic2, "key2", "value2")

        kafkaProducer.send(record1).get()
        kafkaProducer.send(record2).get()

        // Wait for both messages to be processed
        val messagesProcessed = latch.await(10, TimeUnit.SECONDS)

        // Assert
        assertEquals(true, messagesProcessed, "Both messages should have been processed")

        // Verify that the dispatcher was called twice
        verify(dispatchService, times(2)).dispatchEvents(
            Mockito.any(),
            Mockito.any(),
            Mockito.any()
        )

        // Clean up
        try {
            registry.stopListener(topic2)
        } catch (e: Exception) {
            // Ignore exceptions during cleanup
        }
    }

    @Test
    fun `test stopping and restarting listener`() {
        // Arrange - Register and start a listener
        val request = ListenerRegistrationRequest(
            topic = testTopic,
            groupId = testGroupId,
            key = Broker.BrokerFormat.STRING,
            value = Broker.BrokerFormat.STRING
        )

        registry.registerListener(request)
        registry.startListener(testTopic)

        // Use a latch to wait for the first message to be processed
        val firstMessageLatch = CountDownLatch(1)

        // Configure the dispatch service mock to count down the latch
        Mockito.doAnswer {
            firstMessageLatch.countDown()
            null
        }.`when`(dispatchService).dispatchEvents(Mockito.any(), Mockito.any(), Mockito.any())

        // Send first message and wait for processing
        val record1 = ProducerRecord(testTopic, testKey, testValue)
        kafkaProducer.send(record1).get()

        val firstMessageProcessed = firstMessageLatch.await(10, TimeUnit.SECONDS)
        assertEquals(true, firstMessageProcessed, "First message should have been processed")

        // Stop the listener
        registry.stopListener(testTopic)

        // Reset mock
        Mockito.reset(dispatchService)

        // Send a message while listener is stopped - this should not be processed
        val record2 = ProducerRecord(testTopic, "key2", "value2")
        kafkaProducer.send(record2).get()

        // Verify that the dispatcher was not called again
        verify(dispatchService, times(0)).dispatchEvents(
            Mockito.any(),
            Mockito.any(),
            Mockito.any()
        )

        // Restart the listener
        registry.startListener(testTopic)

        // Use a new latch for the next message
        val secondMessageLatch = CountDownLatch(1)

        // Configure the dispatch service mock again
        Mockito.doAnswer {
            secondMessageLatch.countDown()
            null
        }.`when`(dispatchService).dispatchEvents(Mockito.any(), Mockito.any(), Mockito.any())

        // Send a third message
        val record3 = ProducerRecord(testTopic, "key3", "value3")
        kafkaProducer.send(record3).get()

        // Wait for processing
        val thirdMessageProcessed = secondMessageLatch.await(10, TimeUnit.SECONDS)
        assertEquals(true, thirdMessageProcessed, "Third message should have been processed")

        // Verify that the dispatcher was called again
        verify(dispatchService, times(1)).dispatchEvents(
            Mockito.any(),
            Mockito.any(),
            Mockito.any()
        )
    }
}