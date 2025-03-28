package paladin.router.pojo.configuration.brokers

import java.io.Serializable

data class EncryptedBrokerAuthConfig(
    val securityProtocol: String?,   // Common: SSL, PLAINTEXT, etc.
    val username: String?,
    val password: String?,
    val accessKeyId: String?,        // SQS-like authentication
    val secretAccessKey: String?,
    val sslKeystorePath: String?,
    val sslKeystorePassword: String?,
    val sslTruststorePath: String?,
    val sslTruststorePassword: String?,
    val brokerSpecificConfig: Map<String, String> = emptyMap() // Extra fields for future brokers
) : Serializable