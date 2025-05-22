package paladin.router.services.dispatch

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import io.mockk.verify
import org.junit.jupiter.api.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.junit.jupiter.Testcontainers
import paladin.router.enums.configuration.Broker
import paladin.router.models.dispatch.DispatchTopic
import paladin.router.models.dispatch.DispatchTopicRequest
import paladin.router.services.producers.ProducerService
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest
import util.TestLogAppender
import util.brokers.ProducerCreationFactory
import util.kafka.KafkaClusterManager
import util.rabbit.RabbitClusterManager
import util.sqs.SqsClusterManager
import java.util.*
import kotlin.test.assertTrue

@SpringBootTest
@DirtiesContext
@Testcontainers
@ActiveProfiles("test")
class DispatchIntegrationTest {

    private lateinit var testAppender: TestLogAppender
    private var logger: KLogger = KotlinLogging.logger {}
    private lateinit var logbackLogger: Logger

    @Autowired
    private lateinit var dispatchService: DispatchService

    @Autowired
    private lateinit var producerService: ProducerService

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
        private const val SQS_CLUSTER_1 = "sqs-cluster-1"
        private const val KAFKA_CLUSTER_1 = "kafka-cluster-1"
        private const val KAFKA_CLUSTER_2 = "kafka-cluster-2"
        private const val RABBIT_MQ_CLUSTER_1 = "rabbitmq-cluster-1"

        private val kafkaClusterManager = KafkaClusterManager()
        private val sqsClusterManager = SqsClusterManager()
        private val rabbitMqClusterManager = RabbitClusterManager()

        @BeforeAll
        @JvmStatic
        fun setupClusters() {
            // Setup code here
            kafkaClusterManager.init(KAFKA_CLUSTER_1, includeSchemaRegistry = true)
            kafkaClusterManager.init(KAFKA_CLUSTER_2, includeSchemaRegistry = false)
            sqsClusterManager.init(SQS_CLUSTER_1)
            rabbitMqClusterManager.init(RABBIT_MQ_CLUSTER_1)
        }

        @AfterAll
        @JvmStatic
        fun shutdownClusters() {
            // Teardown code here
            kafkaClusterManager.cleanupAll()
            sqsClusterManager.cleanupAll()
            rabbitMqClusterManager.cleanupAll()
        }

        @DynamicPropertySource
        @JvmStatic
        fun dynamicProperties(registry: org.springframework.test.context.DynamicPropertyRegistry) {
            // Register properties for Kafka Cluster 1
            kafkaClusterManager.registerProperties(KAFKA_CLUSTER_1, registry)
            // Register properties for Kafka Cluster 2
            kafkaClusterManager.registerProperties(KAFKA_CLUSTER_2, registry)
            // Register properties for SQS Cluster 1
            sqsClusterManager.registerProperties(SQS_CLUSTER_1, registry)
            // Register properties for RabbitMQ Cluster 1
            rabbitMqClusterManager.registerProperties(RABBIT_MQ_CLUSTER_1, registry)
        }

    }

    @Test
    fun `handle dispatch to multiple receiver brokers`(
    ) {
        val sourceTopic: String = "test-topic-${UUID.randomUUID()}"

        val (sqsTopic1, sqsQueue1) = "sqs-test-topic-${UUID.randomUUID()}".let {
            Pair(it, sqsClusterManager.createQueue(SQS_CLUSTER_1, it))
        }
        val (rabbitTopic1, rabbitQueue1) = "rabbit-test-topic-${UUID.randomUUID()}".let {
            Pair(it, rabbitMqClusterManager.createQueue(RABBIT_MQ_CLUSTER_1, it))
        }
        val kafkaTopic1 = "kafka-test-topic-${UUID.randomUUID()}".also {
            kafkaClusterManager.createTopic(KAFKA_CLUSTER_1, it)
        }

        val kafkaTopic2 = "kafka-test-topic-${UUID.randomUUID()}".also {
            kafkaClusterManager.createTopic(KAFKA_CLUSTER_2, it)
        }

        // Set up Dispatchers
        val kafkaProducer1 = ProducerCreationFactory.fromKafka(
            name = "kafka-producer-1",
            cluster = kafkaClusterManager.getCluster(KAFKA_CLUSTER_1),
            keySerializationFormat = Broker.ProducerFormat.STRING,
            valueSerializationFormat = Broker.ProducerFormat.STRING,
            requireKey = true
        )

        val kafkaProducer2 = ProducerCreationFactory.fromKafka(
            name = "kafka-producer-2",
            cluster = kafkaClusterManager.getCluster(KAFKA_CLUSTER_2),
            keySerializationFormat = Broker.ProducerFormat.STRING,
            valueSerializationFormat = Broker.ProducerFormat.STRING,
            requireKey = true
        )

        val sqsProducer = ProducerCreationFactory.fromSqs(
            name = "sqs-producer-1",
            cluster = sqsClusterManager.getCluster(SQS_CLUSTER_1),
            valueSerializationFormat = Broker.ProducerFormat.STRING,
            requireKey = false
        )

        val rabbitProducer = ProducerCreationFactory.fromRabbit(
            name = "rabbit-producer-1",
            cluster = rabbitMqClusterManager.getCluster(RABBIT_MQ_CLUSTER_1),
            valueSerializationFormat = Broker.ProducerFormat.STRING,
            queue = rabbitQueue1,
            requireKey = false
        )

        // Set up Dispatchers and assert successful connection with given configuration properties
        val kafka1Dispatcher = producerService.registerProducer(kafkaProducer1).also {
            assertTrue { it.testConnection() }
        }
        val kafka2Dispatcher = producerService.registerProducer(kafkaProducer2).also {
            assertTrue { it.testConnection() }
        }
        val sqsDispatcher = producerService.registerProducer(sqsProducer).also {
            assertTrue { it.testConnection() }
        }
        val rabbitDispatcher = producerService.registerProducer(rabbitProducer).also {
            assertTrue { it.testConnection() }
        }


        val kafkaBroker1DispatchTopicRequest = DispatchTopicRequest(
            dispatcher = kafkaProducer1.producerName,
            sourceTopic = sourceTopic,
            destinationTopic = kafkaTopic1,
            key = Broker.ProducerFormat.STRING,
            value = Broker.ProducerFormat.STRING
        )

        val sqsProducerDispatchTopicRequest = DispatchTopicRequest(
            dispatcher = sqsProducer.producerName,
            sourceTopic = sourceTopic,
            destinationTopic = sqsTopic1,
            key = Broker.ProducerFormat.STRING,
            value = Broker.ProducerFormat.STRING
        )

        val rabbitProducerDispatchTopicRequest = DispatchTopicRequest(
            dispatcher = rabbitProducer.producerName,
            sourceTopic = sourceTopic,
            destinationTopic = rabbitTopic1,
            key = Broker.ProducerFormat.STRING,
            value = Broker.ProducerFormat.STRING
        )

        // Allocate topics to dispatchers
        dispatchService.addDispatcherTopic(kafkaBroker1DispatchTopicRequest)
        dispatchService.addDispatcherTopic(sqsProducerDispatchTopicRequest)
        dispatchService.addDispatcherTopic(rabbitProducerDispatchTopicRequest)

        // Mock an Event Listener dispatching an event
        dispatchService.dispatchEvents(
            key = "test-key",
            value = "test-value",
            topic = sourceTopic,
        )

        // Assert message has been dispatched
        verify(exactly = 1) { kafka1Dispatcher.dispatch("test-key", "test-value", any<DispatchTopic>()) }
        verify(exactly = 0) { kafka2Dispatcher.dispatch(any<String>(), any<String>(), any<DispatchTopic>()) }
        verify(exactly = 1) { sqsDispatcher.dispatch("test-value", any<DispatchTopic>()) }
        verify(exactly = 1) { rabbitDispatcher.dispatch("test-value", any<DispatchTopic>()) }

        // Assert message was received by testContainer broker
        val sqsClient = sqsClusterManager.getCluster(SQS_CLUSTER_1).client
        val rabbitClient = rabbitMqClusterManager.getCluster(RABBIT_MQ_CLUSTER_1).client
        val kafka1Client = kafkaClusterManager.getCluster(KAFKA_CLUSTER_1).client


        val sqsMessages1 = sqsClient.receiveMessage(
            ReceiveMessageRequest.builder()
                .queueUrl(sqsQueue1)
                .maxNumberOfMessages(1)
                .waitTimeSeconds(10)
                .build()
        ).messages()

        // Receive messages from RabbitMQ
        val receivedRabbitMessage1 = rabbitmqCluster1Template.receiveAndConvert(rabbitQueue1, 10000) as String?

        // Receive messages from Kafka
        val consumerProps1 = mapOf(
            "bootstrap.servers" to kafkaClusterManager.getCluster(KAFKA_CLUSTER_1).container.bootstrapServers,

            "group.id" to "test-group-1",
            "key.deserializer" to "org.apache.kafka.common.serialization.StringDeserializer",
            "value.deserializer" to "org.apache.kafka.common.serialization.StringDeserializer",
            "auto.offset.reset" to "earliest"
        )
        val consumerProps2 = mapOf(
            "bootstrap.servers" to kafkaClusterManager.getCluster(KAFKA_CLUSTER_2).container.bootstrapServers,
            "group.id" to "test-group-2",
            "key.deserializer" to "org.apache.kafka.common.serialization.StringDeserializer",
            "value.deserializer" to "org.apache.kafka.common.serialization.StringDeserializer",
            "auto.offset.reset" to "earliest"
        )
        val consumer1 = org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(consumerProps1)
        val consumer2 = org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(consumerProps2)
        consumer1.subscribe(listOf(kafkaTopic1))
        consumer2.subscribe(listOf(kafkaTopic2))

        val kafkaRecords1 = consumer1.poll(Duration.ofSeconds(10))
        val kafkaRecords2 = consumer2.poll(Duration.ofSeconds(10))

        // Assert SQS messages
        assert(sqsMessages1.size == 1)
        assertEquals(sqsMessages1.first().body(), sqsMessage1)

        // Assert RabbitMQ messages
        receivedRabbitMessage1.let {
            assertNotNull(it)
            assertEquals(it, rabbitMessage1)
        }
        assert(receivedRabbitMessage1 != null)
        assertEquals(receivedRabbitMessage1, rabbitMessage1)

        kafkaRecords1.let {
            assert(it.count() == 1)
            assertEquals(it.first().value(), kafkaMessage1)
        }

        kafkaRecords2.let {
            assert(it.count() == 1)
            assertEquals(it.first().value(), kafkaMessage2)
        }

        // Clean up Kafka consumers
        consumer1.close()
        consumer2.close()
    }
}