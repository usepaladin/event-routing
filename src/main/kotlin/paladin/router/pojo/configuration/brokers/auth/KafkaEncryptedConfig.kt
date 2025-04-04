package paladin.router.pojo.configuration.brokers.auth

data class KafkaEncryptedConfig(
    val bootstrapServers: String? = null,
    val securityProtocol: String? = null,
    val saslMechanism: String? = null,
    val saslJaasConfig: String? = null,
    val sslTruststoreLocation: String? = null,
    val sslTruststorePassword: String? = null,
    val sslKeystoreLocation: String? = null,
    val sslKeystorePassword: String? = null
):EncryptedBrokerConfig