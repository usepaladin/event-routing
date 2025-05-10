package paladin.router.services.listener

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.json.JsonSchema
import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.spyk
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.Network
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.kafka.ConfluentKafkaContainer
import org.testcontainers.utility.DockerImageName
import paladin.avro.ChangeEventData
import paladin.avro.MockKeyAv
import paladin.router.enums.configuration.Broker
import paladin.router.models.listener.AdditionalConsumerProperties
import paladin.router.models.listener.EventListener
import paladin.router.models.listener.ListenerRegistrationRequest
import paladin.router.repository.EventListenerRepository
import paladin.router.services.dispatch.DispatchService
import util.TestLogAppender
import util.kafka.SchemaRegistrationOperation
import util.kafka.SchemaRegistryContainer
import util.kafka.SchemaRegistryFactory
import util.kafka.TestKafkaProducerFactory
import util.mock.Operation
import util.mock.User
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

    private lateinit var testAppender: TestLogAppender
    private var logger: KLogger = KotlinLogging.logger {}
    private lateinit var logbackLogger: Logger


    @BeforeEach
    fun setup() {
        logbackLogger = LoggerFactory.getLogger(logger.name) as Logger
        testAppender = TestLogAppender.factory(logbackLogger, Level.DEBUG)
    }

    companion object {
        private val logger = KotlinLogging.logger {}
        private val network = Network.newNetwork()

        // Give Kafka a consistent network alias
        private val kafkaContainer = ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"))
            .withListener("kafka:19092")
            .withNetwork(network)
            .withReuse(true)


        private val schemaRegistryContainer =
            SchemaRegistryContainer(DockerImageName.parse("confluentinc/cp-schema-registry:7.4.0"))
                .withNetwork(network)

        @BeforeAll
        @JvmStatic
        fun startKafka() {
            kafkaContainer.start()
            logger.info { "Kafka container started with bootstrap servers: ${kafkaContainer.bootstrapServers}" }

            // Register and start schema registry after Kafka is running
            schemaRegistryContainer.withKafka(kafkaContainer).start()

            logger.info { "Schema Registry started with URL: ${schemaRegistryContainer.schemaRegistryUrl}" }
        }

        @AfterAll
        @JvmStatic
        fun stopKafka() {
            schemaRegistryContainer.stop()
            kafkaContainer.stop()
            network.close()
            logger.info { "Containers stopped and network closed" }
        }

        @DynamicPropertySource
        @JvmStatic
        fun kafkaProperties(registry: DynamicPropertyRegistry) {
            registry.add("spring.kafka.bootstrap-servers") { kafkaContainer.bootstrapServers }
            registry.add("spring.kafka.schema-registry-url") { schemaRegistryContainer.schemaRegistryUrl }
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
        val topic = "test-topic-${UUID.randomUUID()}"
        val groupId = "test-group"
        val key = "test-key"
        val value = "test-value"
        val latch = CountDownLatch(1)

        // Mock DispatchService to verify dispatchEvents call
        val spiedDispatchService = spyk(dispatchService)
        val (registry: EventListenerRegistry, listener: EventListener) = configureEventListener(
            spiedDispatchService,
            topic,
            groupId,
            Broker.BrokerFormat.STRING,
            Broker.BrokerFormat.STRING
        )

        coEvery {
            spiedDispatchService.dispatchEvents(any<String>(), any<String>(), any<EventListener>())
        } coAnswers {
            latch.countDown()
            Any()
        }

        @Suppress("UNCHECKED_CAST")
        val template = TestKafkaProducerFactory.createKafkaTemplate(
            kafkaContainer,
            Broker.BrokerFormat.STRING,
            Broker.BrokerFormat.STRING
        ) as KafkaTemplate<String, String>


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

        // Mock DispatchService to verify dispatchEvents call
        val spiedDispatchService: DispatchService = spyk(dispatchService)
        val (registry: EventListenerRegistry, _: EventListener) = configureEventListener(
            spiedDispatchService,
            topic,
            groupId,
            Broker.BrokerFormat.STRING,
            Broker.BrokerFormat.AVRO
        )

        coEvery {
            spiedDispatchService.dispatchEvents(any<String>(), any<String>(), any<EventListener>())
        } coAnswers {
            latch.countDown()
            Any()
        }

        @Suppress("UNCHECKED_CAST")
        val template = TestKafkaProducerFactory.createKafkaTemplate(
            kafkaContainer,
            Broker.BrokerFormat.STRING,
            Broker.BrokerFormat.AVRO
        ) as KafkaTemplate<String, String>


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

    @Test
    fun `event listener should handle and deserialize avro payloads`() {
        val topic = "test-topic"
        val groupId = "test-group"
        val latch = CountDownLatch(1)

        SchemaRegistryFactory.init(
            schemaRegistryContainer.schemaRegistryUrl,
            listOf(
                SchemaRegistrationOperation(
                    AvroSchema(MockKeyAv.`SCHEMA$`),
                    topic,
                    SchemaRegistrationOperation.SchemaType.KEY
                ),
                SchemaRegistrationOperation(
                    AvroSchema(ChangeEventData.`SCHEMA$`),
                    topic,
                    SchemaRegistrationOperation.SchemaType.VALUE
                )
            )
        )

        // Mock DispatchService to verify dispatchEvents call
        val spiedDispatchService: DispatchService = spyk(dispatchService)

        val config = AdditionalConsumerProperties(
            autoOffsetReset = "earliest",
            enableAutoCommit = true,
            maxPollRecords = 10,
            maxPollIntervalMs = 300000,
            sessionTimeoutMs = 10000,
            schemaRegistryUrl = schemaRegistryContainer.schemaRegistryUrl
        )

        val (registry, listener) = configureEventListener(
            spiedDispatchService,
            topic,
            groupId,
            Broker.BrokerFormat.AVRO,
            Broker.BrokerFormat.AVRO,
            config
        )

        coEvery {
            spiedDispatchService.dispatchEvents(any<String>(), any<String>(), any<EventListener>())
        } coAnswers {
            latch.countDown()
            Any()
        }

        @Suppress("UNCHECKED_CAST")
        val template = TestKafkaProducerFactory.createKafkaTemplate(
            kafkaContainer,
            Broker.BrokerFormat.STRING,
            Broker.BrokerFormat.AVRO,
            schemaRegistryContainer.schemaRegistryUrl
        ) as KafkaTemplate<SpecificRecord, SpecificRecord>

        assertNotNull(listener.id, "Listener ID should be set after registration")
        registry.startListener(listener.topic)

        val key: MockKeyAv = TestKafkaProducerFactory.mockAvroKey()
        val payload: ChangeEventData = TestKafkaProducerFactory.mockAvroPayload()
        template.send(listener.topic, key, payload).get()
        // Assert: Verify message processing
        val processed = latch.await(5, TimeUnit.SECONDS)
        assertEquals(true, processed, "Message should be processed within 5 seconds")

        // Verify dispatchService was called with correct parameters
        coVerify(exactly = 1) {
            spiedDispatchService.dispatchEvents(key, payload, listener)
        }

        // Cleanup
        registry.stopListener(topic)
    }

    @Test
    fun `event listener should handle and deserialize combination consumers`() {
        val topic = "test-topic"
        val groupId = "test-group"
        val latch = CountDownLatch(1)

        SchemaRegistryFactory.init(
            schemaRegistryContainer.schemaRegistryUrl,
            listOf(
                SchemaRegistrationOperation(
                    AvroSchema(ChangeEventData.`SCHEMA$`),
                    topic,
                    SchemaRegistrationOperation.SchemaType.VALUE
                )
            )
        )

        // Mock DispatchService to verify dispatchEvents call
        val spiedDispatchService: DispatchService = spyk(dispatchService)

        val config = AdditionalConsumerProperties(
            autoOffsetReset = "earliest",
            enableAutoCommit = true,
            maxPollRecords = 10,
            maxPollIntervalMs = 300000,
            sessionTimeoutMs = 10000,
            schemaRegistryUrl = schemaRegistryContainer.schemaRegistryUrl
        )

        val (registry, listener) = configureEventListener(
            spiedDispatchService,
            topic,
            groupId,
            Broker.BrokerFormat.STRING,
            Broker.BrokerFormat.AVRO,
            config
        )

        coEvery {
            spiedDispatchService.dispatchEvents(any<String>(), any<String>(), any<EventListener>())
        } coAnswers {
            latch.countDown()
            Any()
        }

        @Suppress("UNCHECKED_CAST")
        val template = TestKafkaProducerFactory.createKafkaTemplate(
            kafkaContainer,
            Broker.BrokerFormat.STRING,
            Broker.BrokerFormat.AVRO,
            schemaRegistryContainer.schemaRegistryUrl
        ) as KafkaTemplate<String, SpecificRecord>

        assertNotNull(listener.id, "Listener ID should be set after registration")
        registry.startListener(listener.topic)

        val key: String = "test-key"
        val payload: ChangeEventData = TestKafkaProducerFactory.mockAvroPayload()
        template.send(listener.topic, key, payload).get()
        // Assert: Verify message processing
        val processed = latch.await(5, TimeUnit.SECONDS)
        assertEquals(true, processed, "Message should be processed within 5 seconds")

        // Verify dispatchService was called with correct parameters
        coVerify(exactly = 1) {
            spiedDispatchService.dispatchEvents(key, payload, listener)
        }

        // Cleanup
        registry.stopListener(topic)
    }

    @Test
    fun `event listener should handle and deserialize json based payloads`() {

    }

    @Test
    fun `event listener should handle and deserialize json based payloads with associated schema registry`() {
        val topic = "test-topic"
        val groupId = "test-group"
        val latch = CountDownLatch(1)

        SchemaRegistryFactory.init(
            schemaRegistryContainer.schemaRegistryUrl,
            listOf(
                SchemaRegistrationOperation(
                    JsonSchema(Operation.SCHEMA),
                    topic,
                    SchemaRegistrationOperation.SchemaType.KEY
                ),
                SchemaRegistrationOperation(
                    JsonSchema(User.SCHEMA),
                    topic,
                    SchemaRegistrationOperation.SchemaType.VALUE
                )
            )
        )

        // Mock DispatchService to verify dispatchEvents call
        val spiedDispatchService: DispatchService = spyk(dispatchService)

        val config = AdditionalConsumerProperties(
            autoOffsetReset = "earliest",
            enableAutoCommit = true,
            maxPollRecords = 10,
            maxPollIntervalMs = 300000,
            sessionTimeoutMs = 10000,
            schemaRegistryUrl = schemaRegistryContainer.schemaRegistryUrl
        )

        val (registry, listener) = configureEventListener(
            spiedDispatchService,
            topic,
            groupId,
            Broker.BrokerFormat.JSON,
            Broker.BrokerFormat.JSON,
            config
        )

        coEvery {
            spiedDispatchService.dispatchEvents(any<String>(), any<String>(), any<EventListener>())
        } coAnswers {
            latch.countDown()
            Any()
        }

        val template = TestKafkaProducerFactory.createKafkaTemplate(
            kafkaContainer,
            Broker.BrokerFormat.JSON,
            Broker.BrokerFormat.JSON,
            schemaRegistryContainer.schemaRegistryUrl
        )

        assertNotNull(listener.id, "Listener ID should be set after registration")
        registry.startListener(listener.topic)

        val key: Operation = Operation(
            id = UUID.randomUUID().toString(),
            operation = Operation.OperationType.CREATE
        )

        val payload: User = User(
            name = "Test User",
            age = 30,
            email = "email@email.com"
        )

        template.send(listener.topic, key, payload).get()
        // Assert: Verify message processing
        val processed = latch.await(5, TimeUnit.SECONDS)
        assertEquals(true, processed, "Message should be processed within 5 seconds")

        // Verify dispatchService was called with correct parameters
        coVerify(exactly = 1) {
            spiedDispatchService.dispatchEvents(key, payload, listener)
        }

        // Cleanup
        registry.stopListener(topic)
    }

    private fun configureEventListener(
        service: DispatchService,
        topic: String,
        groupId: String,
        key: Broker.BrokerFormat,
        value: Broker.BrokerFormat,
        config: AdditionalConsumerProperties? = null
    ): Pair<EventListenerRegistry, EventListener> {
        // Register EventListener
        val registry = EventListenerRegistry(
            kafkaConsumerFactory,
            eventListenerRepository,
            service,
            logger
        )

        val properties = config ?: AdditionalConsumerProperties(
            autoOffsetReset = "earliest",
            enableAutoCommit = true,
            maxPollRecords = 10,
            maxPollIntervalMs = 300000,
            sessionTimeoutMs = 10000
        )

        val request = ListenerRegistrationRequest(
            topic = topic,
            groupId = groupId,
            key = key,
            value = value,
            runOnStartup = false,
            config = properties
        )

        val listener = registry.registerListener(request)
        return Pair(registry, listener)
    }
}


