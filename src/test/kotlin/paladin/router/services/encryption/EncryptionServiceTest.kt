package paladin.router.services.encryption

import io.github.oshai.kotlinlogging.KLogger
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.extension.ExtendWith
import paladin.router.configuration.properties.EncryptionConfigurationProperties

@ExtendWith(MockKExtension::class)
class EncryptionServiceTest {

    @MockK
    private lateinit var logger: KLogger

    @MockK
    private lateinit var encryptionConfigurationProperties: EncryptionConfigurationProperties

    private lateinit var encryptionService: EncryptionService

    @BeforeEach
    fun setUp(){
        encryptionService = EncryptionService(encryptionConfigurationProperties, logger)
    }
}