package paladin.router.services.listener

import io.github.oshai.kotlinlogging.KotlinLogging
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.spyk
import kotlinx.coroutines.Job
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.kafka.ConfluentKafkaContainer
import org.testcontainers.utility.DockerImageName
import paladin.router.enums.configuration.Broker
import paladin.router.models.listener.EventListener
import paladin.router.models.listener.ListenerRegistrationRequest
import paladin.router.repository.EventListenerRepository
import paladin.router.services.dispatch.DispatchService
import paladin.router.util.TestKafkaProducerFactory
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

@SpringBootTest
@DirtiesContext
@EmbeddedKafka
@ActiveProfiles("test")
@Testcontainers
class EventListenerIntegrationTest {

    companion object {
        private val logger = KotlinLogging.logger {}
        private val kafkaContainer = ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"))

        @BeforeAll
        @JvmStatic
        fun startKafka() {
            kafkaContainer.start()
            logger.info { "Kafka container started with bootstrap servers: ${kafkaContainer.bootstrapServers}" }
        }

        @AfterAll
        @JvmStatic
        fun stopKafka() {
            kafkaContainer.stop()
            logger.info { "Kafka container stopped" }
        }

        @DynamicPropertySource
        @JvmStatic
        fun kafkaProperties(registry: DynamicPropertyRegistry) {
            registry.add("spring.kafka.bootstrap-servers") { kafkaContainer.bootstrapServers }
        }
    }

    @Autowired
    private lateinit var kafkaConsumerFactory: DefaultKafkaConsumerFactory<Any, Any>

    @Autowired
    private lateinit var eventListenerRepository: EventListenerRepository

    @Autowired
    private lateinit var dispatchService: DispatchService

    @Test
    fun `should register and process message through EventListener`() {
        // Arrange
        val topic = "test-topic-${UUID.randomUUID()}"
        val groupId = "test-group"
        val key = "test-key"
        val value = "test-value"
        val latch = CountDownLatch(1)

        // Mock DispatchService to verify dispatchEvents call
        val spiedDispatchService = spyk(dispatchService)
        coEvery {
            spiedDispatchService.dispatchEvents(any<String>(), any<String>(), any<EventListener>())
        } coAnswers {
            latch.countDown()
            Job()
        }

        @Suppress("UNCHECKED_CAST")
        val template = TestKafkaProducerFactory.createKafkaTemplate(
            kafkaContainer,
            Broker.BrokerFormat.STRING,
            Broker.BrokerFormat.STRING
        ) as KafkaTemplate<String, String>

        // Register EventListener
        val registry = EventListenerRegistry(
            kafkaConsumerFactory,
            eventListenerRepository,
            spiedDispatchService,
            KotlinLogging.logger {}
        )

        val request = ListenerRegistrationRequest(
            topic = topic,
            groupId = groupId,
            key = Broker.BrokerFormat.STRING,
            value = Broker.BrokerFormat.STRING
        )

        val listener = registry.registerListener(request)
        assertNotNull(listener.id, "Listener ID should be set after registration")

        // Start the listener
        registry.startListener(topic)

        // Act: Send a message to Kafka
        val record = ProducerRecord(topic, key, value)
        template.send(record).get()

        // Assert: Verify message processing
        val processed = latch.await(5, TimeUnit.SECONDS)
        assertEquals(true, processed, "Message should be processed within 5 seconds")


        // Verify dispatchService was called with correct parameters
        coVerify(exactly = 1) {
            spiedDispatchService.dispatchEvents(key, value, listener)
        }

        // Cleanup
        registry.stopListener(topic)
    }

    @Test
    fun `should handle multiple messages correctly`() {
        // Arrange
        val topic = "multi-message-topic-${UUID.randomUUID()}"
        val groupId = "multi-group"
        val messageCount = 3
        val latch = CountDownLatch(messageCount)

        // Mock DispatchService to count messages
        val spiedDispatchService = spyk(dispatchService)

        @Suppress("UNCHECKED_CAST")
        val template = TestKafkaProducerFactory.createKafkaTemplate(
            kafkaContainer,
            Broker.BrokerFormat.STRING,
            Broker.BrokerFormat.STRING
        ) as KafkaTemplate<String, String>

        // Register EventListener
        val registry = EventListenerRegistry(
            kafkaConsumerFactory,
            eventListenerRepository,
            spiedDispatchService,
            KotlinLogging.logger {}
        )

        val request = ListenerRegistrationRequest(
            topic = topic,
            groupId = groupId,
            key = Broker.BrokerFormat.STRING,
            value = Broker.BrokerFormat.STRING
        )

        registry.registerListener(request)
        registry.startListener(topic)

        // Act: Send multiple messages
        repeat(messageCount) { index ->
            val record: ProducerRecord<String, String> = ProducerRecord(topic, "key-$index", "value-$index")
            template.send(record).get()
        }

        // Assert: Verify all messages are processed
        val allProcessed = latch.await(10, TimeUnit.SECONDS)
        assertEquals(true, allProcessed, "All $messageCount messages should be processed within 10 seconds")

        // Verify dispatchService was called for each message
        coVerify(exactly = messageCount) {
            spiedDispatchService.dispatchEvents(any<String>(), any<String>(), any<EventListener>())
        }

        // Cleanup
        registry.stopListener(topic)
    }
}


