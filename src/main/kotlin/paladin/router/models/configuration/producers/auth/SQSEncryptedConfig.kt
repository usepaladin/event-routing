package paladin.router.models.configuration.producers.auth

import paladin.router.util.Configurable
import software.amazon.awssdk.regions.Region

data class SQSEncryptedConfig(
    var region: Region,
    var accessKey: String,
    var secretKey: String,
    var endpointURL: String? = null,
) : EncryptedProducerConfig {
    override fun updateConfiguration(config: Configurable): SQSEncryptedConfig {
        if (config is SQSEncryptedConfig) {
            this.region = config.region
            this.accessKey = config.accessKey
            this.secretKey = config.secretKey
            this.endpointURL = config.endpointURL
        }
        return this
    }
}