package paladin.router.services.encryption

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.slf4j.LoggerFactory
import paladin.router.configuration.properties.EncryptionConfigurationProperties
import util.TestLogAppender
import util.TestUtilServices
import util.mock.User
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

@ExtendWith(MockKExtension::class)
class EncryptionServiceTest {

    @MockK
    private lateinit var encryptionConfigurationProperties: EncryptionConfigurationProperties

    private lateinit var encryptionService: EncryptionService

    private lateinit var testAppender: TestLogAppender
    private var logger: KLogger = KotlinLogging.logger {}
    private lateinit var logbackLogger: Logger


    @BeforeEach
    fun setUp() {
        logbackLogger = LoggerFactory.getLogger(logger.name) as Logger
        testAppender = TestLogAppender.factory(logbackLogger, Level.DEBUG)
        every { encryptionConfigurationProperties.key } returns Base64.getEncoder().encodeToString(ByteArray(16) { 1 })
        encryptionService = EncryptionService(encryptionConfigurationProperties, TestUtilServices.objectMapper, logger)

    }

    @Test
    fun `should encrypt and decrypt string correctly`() {
        val original = "Hello, secure world!"
        val encrypted = encryptionService.encrypt(original)
        assertNotNull(encrypted)

        val decrypted = encryptionService.decrypt(encrypted)
        assertEquals(original, decrypted)
    }

    @Test
    fun `should encrypt and decrypt object as map`() {
        val obj = mapOf("username" to "admin", "active" to true)
        val encrypted = encryptionService.encryptObject(obj)
        assertNotNull(encrypted)

        val decrypted = encryptionService.decryptObject(encrypted)
        assertEquals(obj["username"], decrypted?.get("username"))
        assertEquals(obj["active"], decrypted?.get("active"))
    }

    @Test
    fun `should decrypt to typed object`() {
        val user = User("Alice", 30)

        val encrypted = encryptionService.encryptObject(user)
        val decrypted = encryptionService.decryptObject(encrypted!!, User::class.java)

        assertNotNull(decrypted)
        assertEquals(user.name, decrypted.name)
        assertEquals(user.age, decrypted.age)
    }

    @Test
    fun `should decrypt using TypeReference`() {
        val original = listOf("one", "two", "three")
        val json = ObjectMapper().writeValueAsString(original)
        val encrypted = encryptionService.encrypt(json)

        val decrypted = encryptionService.decryptObject(encrypted!!, object : TypeReference<List<String>>() {})
        assertEquals(original, decrypted)
    }

    @Test
    fun `should return null on decryption failure with invalid Base64`() {
        val invalidBase64 = "!!not_base64!!"
        val result = encryptionService.decrypt(invalidBase64)
        assertTrue {
            testAppender.logs.any {
                it.level == Level.ERROR
            }
        }
        assertNull(result)
    }

    @Test
    fun `should return null when ciphertext is too short`() {
        val shortCiphertext = Base64.getEncoder().encodeToString(ByteArray(5))
        val result = encryptionService.decrypt(shortCiphertext)
        assertTrue {
            testAppender.logs.any {
                it.level == Level.ERROR && it.message.contains("Invalid ciphertext format")
            }
        }
        assertNull(result)
    }
}