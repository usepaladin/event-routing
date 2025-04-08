package paladin.router.pojo.configuration.brokers.auth

import paladin.router.enums.configuration.SQS.Region
import paladin.router.util.factory.Configurable

data class SQSEncryptedConfig(
    var region: Region,
    var accessKey: String,
    var secretKey: String,
): EncryptedBrokerConfig{
    override fun updateConfiguration(config: Configurable): SQSEncryptedConfig {
        if (config is SQSEncryptedConfig) {
            this.region = config.region
            this.accessKey = config.accessKey
            this.secretKey = config.secretKey
        }
        return this
    }
}