package paladin.router.services.dispatch

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import org.junit.jupiter.api.*
import org.slf4j.LoggerFactory
import org.springframework.amqp.rabbit.core.RabbitTemplate
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.junit.jupiter.Testcontainers
import paladin.router.enums.configuration.Broker
import paladin.router.services.producers.ProducerService
import software.amazon.awssdk.services.sqs.SqsClient
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest
import software.amazon.awssdk.services.sqs.model.SendMessageRequest
import util.TestLogAppender
import util.kafka.KafkaClusterManager
import util.kafka.TestKafkaProducerFactory
import util.rabbit.RabbitClusterManager
import util.sqs.SqsClusterManager
import java.time.Duration
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

@Testcontainers
@SpringBootTest
@ActiveProfiles("test")
@DirtiesContext
class DispatchIntegrationTest {

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

    @Configuration
    class TestConfig {
        // SQS clients
        @Bean(name = ["sqsCluster1Client"])
        fun sqsCluster1Client(): SqsClient {
            return Companion.sqsClusterManager.getClient(SQS_CLUSTER_1)
        }

        // RabbitMQ templates
        @Bean(name = ["rabbitmqCluster1Template"])
        fun rabbitmqCluster1Template(): RabbitTemplate {
            return RabbitTemplate(rabbitMqClusterManager.getConnectionFactory(RABBIT_MQ_CLUSTER_1))
        }

        // Kafka templates
        @Bean(name = ["kafkaCluster1Template"])
        fun kafkaCluster1Template(): KafkaTemplate<String, String> {
            return TestKafkaProducerFactory.createKafkaTemplate(
                kafkaClusterManager.getCluster(KAFKA_CLUSTER_1).container,
                Broker.ProducerFormat.STRING,
                Broker.ProducerFormat.STRING,
                kafkaClusterManager.getCluster(KAFKA_CLUSTER_1).schemaRegistryContainer?.schemaRegistryUrl
            )
        }

        @Bean(name = ["kafkaCluster2Template"])
        fun kafkaCluster2Template(): KafkaTemplate<String, String> {
            return TestKafkaProducerFactory.createKafkaTemplate(
                kafkaClusterManager.getCluster(KAFKA_CLUSTER_2).container,
                Broker.ProducerFormat.STRING,
                Broker.ProducerFormat.STRING
            )
        }
    }


    @Test
    fun `test send and receive messages across multiple SQS, RabbitMQ, and Kafka clusters`(
        @Qualifier("sqsCluster1Client") sqsCluster1Client: SqsClient,
        @Qualifier("rabbitmqCluster1Template") rabbitmqCluster1Template: RabbitTemplate,
        @Qualifier("kafkaCluster1Template") kafkaCluster1Template: KafkaTemplate<String, String>,
        @Qualifier("kafkaCluster2Template") kafkaCluster2Template: KafkaTemplate<String, String>
    ) {
        val sqsTopic1 = "sqs-test-topic-1"
        val rabbitTopic1 = "rabbit-test-topic-1"
        val kafkaTopic1 = "kafka-test-topic-1"
        val kafkaTopic2 = "kafka-test-topic-2"

        // Create SQS queues
        val sqsQueue1 = sqsClusterManager.createQueue(SQS_CLUSTER_1, sqsTopic1)
        val rabbitQueue1 = rabbitMqClusterManager.createQueue(RABBIT_MQ_CLUSTER_1, rabbitTopic1)
        kafkaClusterManager.createTopic(KAFKA_CLUSTER_1, kafkaTopic1)
        kafkaClusterManager.createTopic(KAFKA_CLUSTER_2, kafkaTopic2)

        // Send messages to SQS
        val sqsMessage1 = "SQS Message for Cluster 1"
        sqsCluster1Client.sendMessage(
            SendMessageRequest.builder()
                .queueUrl(sqsQueue1)
                .messageBody(sqsMessage1)
                .build()
        )

        // Send messages to RabbitMQ
        val rabbitMessage1 = "RabbitMQ Message for Cluster 1"
        rabbitmqCluster1Template.convertAndSend("", rabbitQueue1, rabbitMessage1)

        // Send messages to Kafka
        val kafkaMessage1 = "Kafka Message for Cluster 1"
        val kafkaMessage2 = "Kafka Message for Cluster 2"
        kafkaCluster1Template.send(kafkaTopic1, kafkaMessage1).get()
        kafkaCluster2Template.send(kafkaTopic2, kafkaMessage2).get()

        // Receive messages from SQS
        val sqsMessages1 = sqsCluster1Client.receiveMessage(
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

    @Test
    fun `handle dispatch to multiple receiver brokers`(
        @Qualifier("sqsCluster1Client") sqsCluster1Client: SqsClient,
        @Qualifier("rabbitmqCluster1Template") rabbitmqCluster1Template: RabbitTemplate,
        @Qualifier("kafkaCluster1Template") kafkaCluster1Template: KafkaTemplate<String, String>,
        @Qualifier("kafkaCluster2Template") kafkaCluster2Template: KafkaTemplate<String, String>
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
        // Mock an Event Listener
    }
}