package paladin.router.pojo.configuration.brokers.auth

data class SQSEncryptedConfig(
    val accessKey: String,
    val secretKey: String,
): EncryptedBrokerConfig