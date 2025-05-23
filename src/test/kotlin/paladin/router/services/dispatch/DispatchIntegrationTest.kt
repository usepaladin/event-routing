package paladin.router.services.dispatch

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import org.junit.jupiter.api.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.junit.jupiter.Testcontainers
import paladin.avro.ChangeEventData
import paladin.router.enums.configuration.Broker
import paladin.router.models.dispatch.DispatchTopicRequest
import paladin.router.services.producers.ProducerService
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest
import util.TestLogAppender
import util.brokers.ProducerCreationFactory
import util.kafka.KafkaClusterManager
import util.kafka.SchemaRegistrationOperation
import util.kafka.SchemaRegistryFactory
import util.mock.mockAvroPayload
import util.mock.mockModifiedAvroSchema
import util.rabbit.RabbitClusterManager
import util.sqs.SqsClusterManager
import java.time.Duration
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
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

        val (sqsTopic, sqsQueue) = "sqs-test-topic-${UUID.randomUUID()}".let {
            Pair(it, sqsClusterManager.createQueue(SQS_CLUSTER_1, it))
        }
        val (rabbitTopic, rabbitQueue, rabbitExchange) = "rabbit-test-topic-${UUID.randomUUID()}".let {
            val queue = rabbitMqClusterManager.createQueue(RABBIT_MQ_CLUSTER_1, it)
            val exchange =
                rabbitMqClusterManager.createExchange(RABBIT_MQ_CLUSTER_1, it)

            rabbitMqClusterManager.bindQueueToExchange(
                RABBIT_MQ_CLUSTER_1,
                queue,
                exchange,
                "#"
            )

            Triple(it, queue, exchange)
        }
        val kafkaTopic1 = "kafka-test-topic-${UUID.randomUUID()}".also {
            kafkaClusterManager.createTopic(KAFKA_CLUSTER_1, it)
        }

        val kafkaTopic2 = "kafka-test-topic-${UUID.randomUUID()}".also {
            kafkaClusterManager.createTopic(KAFKA_CLUSTER_2, it)
        }

        // Set up Dispatchers
        ProducerCreationFactory.fromKafka(
            name = "kafka-producer-1",
            cluster = kafkaClusterManager.getCluster(KAFKA_CLUSTER_1),
            keySerializationFormat = Broker.ProducerFormat.STRING,
            valueSerializationFormat = Broker.ProducerFormat.STRING,
            requireKey = true
        ).run {
            // Register Producer
            producerService.registerProducer(this).also {
                assertTrue { it.testConnection() }
            }

            // Add Dispatch topic to producer
            DispatchTopicRequest(
                dispatcher = this.producerName,
                sourceTopic = sourceTopic,
                destinationTopic = kafkaTopic1,
                key = Broker.ProducerFormat.STRING,
                value = Broker.ProducerFormat.STRING
            ).run {
                dispatchService.addDispatcherTopic(this)
            }
        }

        ProducerCreationFactory.fromKafka(
            name = "kafka-producer-2",
            cluster = kafkaClusterManager.getCluster(KAFKA_CLUSTER_2),
            keySerializationFormat = Broker.ProducerFormat.STRING,
            valueSerializationFormat = Broker.ProducerFormat.STRING,
            requireKey = true
        ).run {
            // Register Producer
            producerService.registerProducer(this).also {
                assertTrue { it.testConnection() }
            }
            // Do not add Dispatch topic to producer to test logical filtering and dispatcher retrieval
        }

        ProducerCreationFactory.fromSqs(
            name = "sqs-producer-1",
            cluster = sqsClusterManager.getCluster(SQS_CLUSTER_1),
            valueSerializationFormat = Broker.ProducerFormat.STRING,
            requireKey = false
        ).run {
            // Register Producer
            producerService.registerProducer(this).also {
                assertTrue { it.testConnection() }
            }

            // Add Dispatch topic to producer
            DispatchTopicRequest(
                dispatcher = this.producerName,
                sourceTopic = sourceTopic,
                destinationTopic = sqsTopic,
                key = Broker.ProducerFormat.STRING,
                value = Broker.ProducerFormat.STRING
            ).run {
                dispatchService.addDispatcherTopic(this)
            }
        }

        ProducerCreationFactory.fromRabbit(
            name = "rabbit-producer-1",
            cluster = rabbitMqClusterManager.getCluster(RABBIT_MQ_CLUSTER_1),
            valueSerializationFormat = Broker.ProducerFormat.STRING,
            queue = rabbitQueue,
            requireKey = false,
            exchange = rabbitExchange
        ).run {
            // Register Producer
            producerService.registerProducer(this).also {
                assertTrue { it.testConnection() }
            }

            // Add Dispatch topic to producer
            DispatchTopicRequest(
                dispatcher = this.producerName,
                sourceTopic = sourceTopic,
                destinationTopic = rabbitTopic,
                key = Broker.ProducerFormat.STRING,
                value = Broker.ProducerFormat.STRING
            ).run {
                dispatchService.addDispatcherTopic(this)
            }
        }

        // Mock an Event Listener dispatching an event
        val key = "test-key"
        val payload = "test-value"

        dispatchService.dispatchEvents(
            key = key,
            value = payload,
            topic = sourceTopic,
        )

        val sqsCluster = sqsClusterManager.getCluster(SQS_CLUSTER_1)
        val rabbitCluster = rabbitMqClusterManager.getCluster(RABBIT_MQ_CLUSTER_1)

        val sqsMessages = sqsCluster.client.receiveMessage(
            ReceiveMessageRequest.builder()
                .queueUrl(sqsQueue)
                .maxNumberOfMessages(1)
                .waitTimeSeconds(10)
                .build()
        ).messages()

        // Receive messages from RabbitMQ
        val rabbitMessages = rabbitCluster.channel.basicGet(
            rabbitQueue,
            true
        )

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
        assert(sqsMessages.size == 1)
        assertEquals(sqsMessages.first().body(), payload)

        // Assert RabbitMQ messages
        rabbitMessages.let {
            assertNotNull(it)
            assertEquals(it.body.toString(Charsets.UTF_8), payload)
        }

        kafkaRecords1.let {
            assert(it.count() == 1)
            assertEquals(it.first().key(), key)
            assertEquals(it.first().value(), payload)
        }

        assert(kafkaRecords2.count() == 0)

        // Clean up Kafka consumers
        consumer1.close()
        consumer2.close()
    }

    @Test
    fun `handle dispatch with avro serialisation and custom schema matching`() {
        val sourceTopic = "test-topic-${UUID.randomUUID()}"
        val (sqsTopic, sqsQueue) = "sqs-test-topic-${UUID.randomUUID()}".let {
            Pair(it, sqsClusterManager.createQueue(SQS_CLUSTER_1, it))
        }
        val (rabbitTopic, rabbitQueue, rabbitExchange) = "rabbit-test-topic-${UUID.randomUUID()}".let {
            val queue = rabbitMqClusterManager.createQueue(RABBIT_MQ_CLUSTER_1, it)
            val exchange =
                rabbitMqClusterManager.createExchange(RABBIT_MQ_CLUSTER_1, it)

            rabbitMqClusterManager.bindQueueToExchange(
                RABBIT_MQ_CLUSTER_1,
                queue,
                exchange,
                "#"
            )

            Triple(it, queue, exchange)
        }
        val kafkaTopic1 = "kafka-test-topic-${UUID.randomUUID()}".also {
            kafkaClusterManager.createTopic(KAFKA_CLUSTER_1, it)
        }

        // Register Schema inside Kafka container
        kafkaClusterManager.getCluster(KAFKA_CLUSTER_1).schemaRegistryClient?.let {
            SchemaRegistryFactory.init(
                it, listOf(
                    SchemaRegistrationOperation(
                        schema = AvroSchema(ChangeEventData.`SCHEMA$`),
                        topic = kafkaTopic1,
                        type = SchemaRegistrationOperation.SchemaType.VALUE
                    )
                )
            )
        }


        // Set Up Mock Avro Payloads + Schemas
        val mockAvroValue: ChangeEventData = mockAvroPayload()
        // Subset of original schema. Testing custom schema matching
        val modifiedAvroSchema = mockModifiedAvroSchema()

        // Set up Dispatchers
        ProducerCreationFactory.fromKafka(
            name = "kafka-producer-1",
            cluster = kafkaClusterManager.getCluster(KAFKA_CLUSTER_1),
            keySerializationFormat = Broker.ProducerFormat.STRING,
            valueSerializationFormat = Broker.ProducerFormat.AVRO,
            requireKey = true
        ).run {
            // Register Producer
            producerService.registerProducer(this).also {
                assertTrue { it.testConnection() }
            }

            // Add Dispatch topic to producer
            DispatchTopicRequest(
                dispatcher = this.producerName,
                sourceTopic = sourceTopic,
                destinationTopic = kafkaTopic1,
                key = Broker.ProducerFormat.STRING,
                valueSchema = ChangeEventData.`SCHEMA$`.toString(),
                value = Broker.ProducerFormat.AVRO
            ).run {
                dispatchService.addDispatcherTopic(this)
            }
        }

        ProducerCreationFactory.fromSqs(
            name = "sqs-producer-1",
            cluster = sqsClusterManager.getCluster(SQS_CLUSTER_1),
            valueSerializationFormat = Broker.ProducerFormat.AVRO,
            requireKey = false
        ).run {
            // Register Producer
            producerService.registerProducer(this).also {
                assertTrue { it.testConnection() }
            }

            // Add Dispatch topic to producer
            DispatchTopicRequest(
                dispatcher = this.producerName,
                sourceTopic = sourceTopic,
                destinationTopic = sqsTopic,
                key = Broker.ProducerFormat.STRING,
                value = Broker.ProducerFormat.AVRO,
                valueSchema = modifiedAvroSchema
            ).run {
                dispatchService.addDispatcherTopic(this)
            }
        }

        ProducerCreationFactory.fromRabbit(
            name = "rabbit-producer-1",
            cluster = rabbitMqClusterManager.getCluster(RABBIT_MQ_CLUSTER_1),
            valueSerializationFormat = Broker.ProducerFormat.AVRO,
            queue = rabbitQueue,
            requireKey = false,
            exchange = rabbitExchange
        ).run {
            // Register Producer
            producerService.registerProducer(this).also {
                assertTrue { it.testConnection() }
            }

            // Add Dispatch topic to producer
            DispatchTopicRequest(
                dispatcher = this.producerName,
                sourceTopic = sourceTopic,
                destinationTopic = rabbitTopic,
                key = Broker.ProducerFormat.STRING,
                value = Broker.ProducerFormat.AVRO,
                valueSchema = ChangeEventData.`SCHEMA$`.toString()
            ).run {
                dispatchService.addDispatcherTopic(this)
            }
        }

    }

    @Test
    fun `handle dispatch to sqs FIFO queue and de-duplication`() {
    }

    @Test
    fun `handle rabbit sync vs async queues`() {
    }
}