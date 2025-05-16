package paladin.router.models.configuration.brokers.auth

import paladin.router.enums.configuration.sqs.Region
import paladin.router.util.Configurable

data class SQSEncryptedConfig(
    var region: Region,
    var accessKey: String,
    var secretKey: String,
) : EncryptedProducerConfig {
    override fun updateConfiguration(config: Configurable): SQSEncryptedConfig {
        if (config is SQSEncryptedConfig) {
            this.region = config.region
            this.accessKey = config.accessKey
            this.secretKey = config.secretKey
        }
        return this
    }
}