package paladin.router.models.configuration.producers.auth

import paladin.router.util.Configurable

data class KafkaEncryptedConfig(
    var bootstrapServers: String? = null,
    var schemaRegistryUrl: String? = null,
    var securityProtocol: String? = null,
    var saslMechanism: String? = null,
    var saslJaasConfig: String? = null,
    var sslTruststoreLocation: String? = null,
    var sslTruststorePassword: String? = null,
    var sslKeystoreLocation: String? = null,
    var sslKeystorePassword: String? = null
) : EncryptedProducerConfig {
    override fun updateConfiguration(config: Configurable): KafkaEncryptedConfig {
        if (config is KafkaEncryptedConfig) {
            this.bootstrapServers = config.bootstrapServers
            this.securityProtocol = config.securityProtocol
            this.schemaRegistryUrl = config.schemaRegistryUrl
            this.saslMechanism = config.saslMechanism
            this.saslJaasConfig = config.saslJaasConfig
            this.sslTruststoreLocation = config.sslTruststoreLocation
            this.sslTruststorePassword = config.sslTruststorePassword
            this.sslKeystoreLocation = config.sslKeystoreLocation
            this.sslKeystorePassword = config.sslKeystorePassword
        }
        return this
    }
}