package paladin.router.pojo.configuration.brokers.auth

data class MQTTEncryptedConfig(
    val addresses: String? = null,
    val virtualHost: String? = null,
    val username: String? = null,
    val password: String? = null,

): EncryptedBrokerConfig