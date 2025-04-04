package paladin.router.pojo.configuration.brokers.auth

data class RabbitEncryptedConfig (
    val addresses: String? = null,
    val temp: String? = null
): EncryptedBrokerConfig