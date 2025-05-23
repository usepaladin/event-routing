package paladin.router.models.configuration.producers.auth

import paladin.router.util.Configurable

data class RabbitEncryptedConfig(
    var host: String,
    var port: Int = 5672,
    var virtualHost: String = "/",
    var username: String? = null,
    var password: String? = null,
    var sslEnabled: Boolean = false,
) : EncryptedProducerConfig {
    override fun updateConfiguration(config: Configurable): RabbitEncryptedConfig {
        if (config is RabbitEncryptedConfig) {
            this.virtualHost = config.virtualHost
            this.port = config.port
            this.host = config.host
            this.username = config.username
            this.password = config.password
            this.sslEnabled = config.sslEnabled
        }
        return this
    }
}