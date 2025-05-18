package paladin.router.services.brokers

import io.github.oshai.kotlinlogging.KLogger
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.extension.ExtendWith
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import paladin.router.configuration.properties.EncryptionConfigurationProperties
import paladin.router.repository.MessageProducerRepository
import paladin.router.services.dispatch.DispatchService
import paladin.router.services.encryption.EncryptionService
import paladin.router.util.factory.MessageDispatcherFactory
import util.TestUtilServices

@ExtendWith(MockKExtension::class)
class BrokersServiceTest {

    @MockK
    private lateinit var logger: KLogger

    @MockK
    private lateinit var repository: MessageProducerRepository

    @MockK
    private lateinit var dispatchService: DispatchService

    @MockK
    private lateinit var encryptionService: EncryptionService

    @MockK
    private lateinit var serviceEncryptionConfig: EncryptionConfigurationProperties

    @MockK
    private lateinit var kafkaListenerEndpointRegistry: KafkaListenerEndpointRegistry

    @MockK
    private lateinit var messageDispatchServiceTest: MessageDispatcherFactory

    private lateinit var producerService: ProducerService

    @BeforeEach
    fun setUp() {
        producerService = ProducerService(
            repository,
            dispatchService,
            encryptionService,
            logger,
            serviceEncryptionConfig,
            kafkaListenerEndpointRegistry,
            messageDispatchServiceTest,
            TestUtilServices.objectMapper
        )
    }
}