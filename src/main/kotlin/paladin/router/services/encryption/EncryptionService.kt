package paladin.router.services.encryption

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import io.github.oshai.kotlinlogging.KLogger
import org.springframework.stereotype.Service
import paladin.router.configuration.properties.EncryptionConfigurationProperties
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.security.InvalidAlgorithmParameterException
import java.security.InvalidKeyException
import java.security.NoSuchAlgorithmException
import java.security.spec.AlgorithmParameterSpec
import java.util.*
import javax.crypto.*
import javax.crypto.spec.GCMParameterSpec
import javax.crypto.spec.SecretKeySpec

@Service
class EncryptionService(
    private val encryptionConfigurationProperties: EncryptionConfigurationProperties,
    private val objectMapper: ObjectMapper,
    private val logger: KLogger
) {

    //Todo: Migrate to Kubernetes Secret management
    private var encryptionKeyBase64: String? = encryptionConfigurationProperties.key

    private val algorithm = "AES"
    private val cipherTransformation = "AES/GCM/NoPadding" // AES in Galois/Counter Mode (GCM)
    private val gcmTagLength = 128 // GCM Tag Length in bits (128 bits is recommended)
    private val ivLengthBytes = 12 // Recommended IV length for GCM is 12 bytes (96 bits)

    fun encryptObject(data: Any): String? {
        val dataString = objectMapper.writeValueAsString(data)
        return encrypt(dataString)
    }

    fun decryptObject(encryptedData: String): Map<String, Any>? {
        val decryptedString = decrypt(encryptedData)
        return objectMapper.readValue(decryptedString, object : TypeReference<Map<String, Any>>() {})
    }

    fun <T> decryptObject(encryptedData: String, parsedClass: Class<T>): T? {
        val decryptedString = decrypt(encryptedData)
        return objectMapper.readValue(decryptedString, parsedClass)
    }

    fun <T> decryptObject(encryptedData: String, typeReference: TypeReference<T>): T? {
        val decryptedString = decrypt(encryptedData)
        return objectMapper.readValue(decryptedString, typeReference)
    }

    /**
     * Encrypts plaintext using AES/GCM with a key retrieved from Vault.
     *
     * @param data Data to be encrypted (can be a String, byte array, object, etc.)
     * @return Base64 encoded ciphertext (including IV prepended) or null in case of error
     */
    fun encrypt(data: String): String? {
        if (encryptionKeyBase64 == null) {
            logger.error { "Encryption key is not set" }
            throw IllegalStateException("Encryption key is not set")
        }

        return runCatching {
            val encryptionKeyBytes = Base64.getDecoder().decode(encryptionKeyBase64)
            val secretKey = SecretKeySpec(encryptionKeyBytes, algorithm)
            val cipher = Cipher.getInstance(cipherTransformation)

            val iv = generateRandomIV() // Generate a fresh IV for each encryption
            val parameterSpec: AlgorithmParameterSpec = GCMParameterSpec(gcmTagLength, iv)

            cipher.init(Cipher.ENCRYPT_MODE, secretKey, parameterSpec)

            val plaintextBytes = data.toByteArray(StandardCharsets.UTF_8)
            val ciphertextBytes = cipher.doFinal(plaintextBytes)

            // Prepend IV to ciphertext and then Base64 encode the combined result
            val byteBuffer = ByteBuffer.allocate(iv.size + ciphertextBytes.size)
            byteBuffer.put(iv)
            byteBuffer.put(ciphertextBytes)
            Base64.getEncoder().encodeToString(byteBuffer.array())

        }.onFailure { e ->
            logger.error { "${"Encryption failed: {}"} ${e.message}" }
            when (e) {
                is NoSuchAlgorithmException -> logger.error { "Encryption algorithm not found" }
                is NoSuchPaddingException -> logger.error { "Padding exception" }
                is IllegalBlockSizeException -> logger.error { "Illegal block size during encryption" }
                is BadPaddingException -> logger.error { "Bad padding during encryption" }
                is InvalidKeyException -> logger.error { "Invalid encryption key" }
            }
        }.getOrNull()
    }

    /**
     * Decrypts ciphertext (with prepended IV) using AES/GCM with a key retrieved from Vault.
     *
     * @param ciphertextBase64 Base64 encoded ciphertext (including prepended IV)
     * @return Decrypted plaintext String or null if decryption fails
     */
    fun decrypt(ciphertextBase64: String): String? {
        if (encryptionKeyBase64 == null) {
            logger.error { "Encryption key is not set" }
            throw IllegalStateException("Encryption key is not set")
        }

        return runCatching {
            val encryptionKeyBytes = Base64.getDecoder().decode(encryptionKeyBase64)
            val secretKey = SecretKeySpec(encryptionKeyBytes, algorithm)
            val cipher = Cipher.getInstance(cipherTransformation)

            val decodedCiphertextBytes = Base64.getDecoder().decode(ciphertextBase64)

            if (decodedCiphertextBytes.size < ivLengthBytes) {
                logger.error { "Invalid ciphertext format: IV is missing or too short." }
                return@runCatching null
            }

            val ivBytes = decodedCiphertextBytes.copyOfRange(0, ivLengthBytes)
            val actualCiphertextBytes = decodedCiphertextBytes.copyOfRange(ivLengthBytes, decodedCiphertextBytes.size)

            val parameterSpec: AlgorithmParameterSpec =
                GCMParameterSpec(gcmTagLength, ivBytes)

            cipher.init(Cipher.DECRYPT_MODE, secretKey, parameterSpec)
            val decryptedBytes = cipher.doFinal(actualCiphertextBytes)
            String(decryptedBytes, StandardCharsets.UTF_8)

        }.onFailure { e: Throwable ->
            logger.error { "Decryption failed =>  ${e.message} => ${getSpecificDecryptionError(e)}" }
        }.getOrNull()
    }

    private fun getSpecificDecryptionError(ex: Throwable): String {
        return when (ex) {
            is NoSuchAlgorithmException -> "Decryption algorithm not found"
            is AEADBadTagException -> "Authentication tag mismatch (ciphertext integrity compromised or incorrect key)"
            is NoSuchPaddingException -> "Padding exception during decryption"
            is IllegalBlockSizeException -> "Illegal block size during decryption"
            is BadPaddingException -> "Bad padding during decryption (possibly incorrect key or corrupted ciphertext)"
            is InvalidKeyException -> "Invalid decryption key"
            is InvalidAlgorithmParameterException -> "Invalid algorithm parameters during decryption"
            is IllegalArgumentException -> "Illegal argument provided during decryption"
            else -> "Cause => Unknown"
        }
    }


    private fun generateRandomIV(): ByteArray {
        val iv = ByteArray(ivLengthBytes)
        java.security.SecureRandom().nextBytes(iv) // Use SecureRandom for IV generation
        return iv
    }
}