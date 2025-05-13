package paladin.router.services.listener

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import com.fasterxml.jackson.databind.JsonNode
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.json.JsonSchema
import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.spyk
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.jupiter.api.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Bean
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.kafka.ConfluentKafkaContainer
import paladin.avro.ChangeEventData
import paladin.avro.MockKeyAv
import paladin.router.enums.configuration.Broker
import paladin.router.models.listener.AdditionalConsumerProperties
import paladin.router.models.listener.EventListener
import paladin.router.models.listener.ListenerRegistrationRequest
import paladin.router.repository.EventListenerRepository
import paladin.router.services.dispatch.DispatchService
import util.TestLogAppender
import util.kafka.*
import util.mock.Operation
import util.mock.User
import util.mock.mockAvroKey
import util.mock.mockAvroPayload
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

    @AfterEach
    fun tearDown() {
        logbackLogger.detachAppender(testAppender)
        testAppender.stop()
    }

    companion object {
        private const val KAFKA_CLUSTER_1 = "test-cluster-1"

        private val kafkaManager = KafkaClusterManager()

        @BeforeAll
        @JvmStatic
        fun setupClusters() {
            kafkaManager.init(KAFKA_CLUSTER_1, includeSchemaRegistry = true)
        }

        @AfterAll
        @JvmStatic
        fun shutdownClusters() {
            kafkaManager.cleanupAll()
        }

        @DynamicPropertySource
        @JvmStatic
        fun overrideConfigurations(registry: DynamicPropertyRegistry) {
            kafkaManager.registerProperties(KAFKA_CLUSTER_1, registry, "spring.kafka.clusters.$KAFKA_CLUSTER_1\"")
        }
    }

    // Kafka consumer factories
    @Bean(name = ["kafkaClusterConsumerFactory"])
    fun kafkaCluster1ConsumerFactory(): DefaultKafkaConsumerFactory<Any, Any> {
        return DefaultKafkaConsumerFactory(
            mapOf(
                "bootstrap.servers" to kafkaManager.getCluster(KAFKA_CLUSTER_1).container.bootstrapServers,
                "group.id" to "test-group-1",
                "key.deserializer" to "org.apache.kafka.common.serialization.StringDeserializer",
                "value.deserializer" to "org.apache.kafka.common.serialization.StringDeserializer",
                "auto.offset.reset" to "earliest"
            )
        )
    }

    @Autowired
    private lateinit var eventListenerRepository: EventListenerRepository

    @Autowired
    private lateinit var dispatchService: DispatchService

    @Test
    fun `should register and process message through EventListener`(
        @Qualifier("kafkaClusterConsumerFactory") kafkaConsumerFactory: DefaultKafkaConsumerFactory<Any, Any>
    ) {
        val (kafka, schemaRegistry) = getKafkaInstance(KAFKA_CLUSTER_1)
        if (schemaRegistry == null) {
            throw IllegalStateException("Schema Registry is not available")
        }

        val topic = "test-topic-${UUID.randomUUID()}".also {
            kafkaManager.createTopic(KAFKA_CLUSTER_1, it)
        }
        val groupId = "test-group"
        val key = "test-key"
        val value = "test-value"
        val latch = CountDownLatch(1)


        // Mock DispatchService to verify dispatchEvents call
        val spiedDispatchService = spyk(dispatchService)
        val (registry: EventListenerRegistry, listener: EventListener) = configureEventListener(
            kafkaConsumerFactory,
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
        }

        val template = TestKafkaProducerFactory.createKafkaTemplate<String, String>(
            kafka,
            Broker.BrokerFormat.STRING,
            Broker.BrokerFormat.STRING
        )

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
    fun `should handle multiple messages correctly`(
        @Qualifier("kafkaClusterConsumerFactory") kafkaConsumerFactory: DefaultKafkaConsumerFactory<Any, Any>
    ) {
        val (kafka, schemaRegistry) = getKafkaInstance(KAFKA_CLUSTER_1)

        if (schemaRegistry == null) {
            throw IllegalStateException("Schema Registry is not available")
        }

        // Arrange
        val topic = "multi-message-topic-${UUID.randomUUID()}"
        val groupId = "multi-group"
        val messageCount = 3
        val latch = CountDownLatch(messageCount)

        // Mock DispatchService to verify dispatchEvents call
        val spiedDispatchService: DispatchService = spyk(dispatchService)
        val (registry: EventListenerRegistry, _: EventListener) = configureEventListener(
            kafkaConsumerFactory,
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
        }

        val template = TestKafkaProducerFactory.createKafkaTemplate<String, String>(
            kafka,
            Broker.BrokerFormat.STRING,
            Broker.BrokerFormat.STRING
        )


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
    fun `event listener should handle and deserialize avro payloads`(
        @Qualifier("kafkaClusterConsumerFactory") kafkaConsumerFactory: DefaultKafkaConsumerFactory<Any, Any>
    ) {
        val (kafka, schemaRegistry) = getKafkaInstance(KAFKA_CLUSTER_1)

        if (schemaRegistry == null) {
            throw IllegalStateException("Schema Registry is not available")
        }

        val topic = "test-topic-${UUID.randomUUID()}"
        val groupId = "test-group"
        val latch = CountDownLatch(1)

        SchemaRegistryFactory.init(
            schemaRegistry.schemaRegistryUrl,
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
            schemaRegistryUrl = schemaRegistry.schemaRegistryUrl
        )

        val (registry, listener) = configureEventListener(
            kafkaConsumerFactory,
            spiedDispatchService,
            topic,
            groupId,
            Broker.BrokerFormat.AVRO,
            Broker.BrokerFormat.AVRO,
            config
        )

        coEvery {
            spiedDispatchService.dispatchEvents(any<SpecificRecord>(), any<SpecificRecord>(), any<EventListener>())
        } coAnswers {
            latch.countDown()
        }

        val template = TestKafkaProducerFactory.createKafkaTemplate<SpecificRecord, SpecificRecord>(
            kafka,
            Broker.BrokerFormat.AVRO,
            Broker.BrokerFormat.AVRO,
            schemaRegistry.schemaRegistryUrl
        )

        assertNotNull(listener.id, "Listener ID should be set after registration")
        registry.startListener(listener.topic)

        val key: MockKeyAv = mockAvroKey()
        val payload: ChangeEventData = mockAvroPayload()
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
    fun `event listener should handle and deserialize combination consumers`(
        @Qualifier("kafkaClusterConsumerFactory") kafkaConsumerFactory: DefaultKafkaConsumerFactory<Any, Any>
    ) {
        val (kafka, schemaRegistry) = getKafkaInstance(KAFKA_CLUSTER_1)

        if (schemaRegistry == null) {
            throw IllegalStateException("Schema Registry is not available")
        }

        val topic = "test-topic-${UUID.randomUUID()}"
        val groupId = "test-group"
        val latch = CountDownLatch(1)

        SchemaRegistryFactory.init(
            schemaRegistry.schemaRegistryUrl,
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
            schemaRegistryUrl = schemaRegistry.schemaRegistryUrl
        )

        val (registry, listener) = configureEventListener(
            kafkaConsumerFactory,
            spiedDispatchService,
            topic,
            groupId,
            Broker.BrokerFormat.STRING,
            Broker.BrokerFormat.AVRO,
            config
        )

        coEvery {
            spiedDispatchService.dispatchEvents(any<String>(), any<SpecificRecord>(), any<EventListener>())
        } coAnswers {
            latch.countDown()
        }

        val template = TestKafkaProducerFactory.createKafkaTemplate<String, SpecificRecord>(
            kafka,
            Broker.BrokerFormat.STRING,
            Broker.BrokerFormat.AVRO,
            schemaRegistry.schemaRegistryUrl
        )

        assertNotNull(listener.id, "Listener ID should be set after registration")
        registry.startListener(listener.topic)

        val key: String = "test-key"
        val payload: ChangeEventData = mockAvroPayload()
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
    fun `event listener should handle failed schema validation for Json Schemas`(
        @Qualifier("kafkaClusterConsumerFactory") kafkaConsumerFactory: DefaultKafkaConsumerFactory<Any, Any>
    ) {
        val (kafka, schemaRegistry) = getKafkaInstance(KAFKA_CLUSTER_1)

        if (schemaRegistry == null) {
            throw IllegalStateException("Schema Registry is not available")
        }

        val topic = "test-topic-${UUID.randomUUID()}"
        val groupId = "test-group"
        val latch = CountDownLatch(1)

        SchemaRegistryFactory.init(
            schemaRegistry.schemaRegistryUrl,
            listOf(
                SchemaRegistrationOperation(
                    // Mismatched Schema
                    JsonSchema(User.SCHEMA),
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
            schemaRegistryUrl = schemaRegistry.schemaRegistryUrl
        )

        val (registry, listener) = configureEventListener(
            kafkaConsumerFactory,
            spiedDispatchService,
            topic,
            groupId,
            Broker.BrokerFormat.JSON,
            Broker.BrokerFormat.JSON,
            config
        )

        coEvery {
            spiedDispatchService.dispatchEvents(any<JsonNode>(), any<JsonNode>(), any<EventListener>())
        } coAnswers {
            latch.countDown()
        }

        val template = TestKafkaProducerFactory.createKafkaTemplate<Operation, User>(
            kafka,
            Broker.BrokerFormat.JSON,
            Broker.BrokerFormat.JSON,
            schemaRegistry.schemaRegistryUrl
        )

        assertNotNull(listener.id, "Listener ID should be set after registration")
        registry.startListener(listener.topic)

        val key =
            Operation(
                id = UUID.randomUUID().toString(),
                operation = Operation.OperationType.CREATE
            )

        val payload = User(
            name = "Test User",
            age = 30,
            email = "email@email.com"
        )
        assertThrows<Exception> {
            template.send(listener.topic, key, payload).get()
        }

        // Cleanup
        registry.stopListener(topic)
    }

    private fun configureEventListener(
        factory: DefaultKafkaConsumerFactory<Any, Any>,
        service: DispatchService,
        topic: String,
        groupId: String,
        key: Broker.BrokerFormat,
        value: Broker.BrokerFormat,
        config: AdditionalConsumerProperties? = null
    ): Pair<EventListenerRegistry, EventListener> {
        // Register EventListener
        val registry = EventListenerRegistry(
            factory,
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

    private fun getKafkaInstance(id: String): Pair<ConfluentKafkaContainer, SchemaRegistryContainer?> {
        return kafkaManager.getCluster(id).let {
            Pair(it.container, it.schemaRegistryContainer)
        }
    }

}



