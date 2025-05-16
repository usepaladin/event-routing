package paladin.router.models.configuration.brokers.auth

import paladin.router.util.Configurable

data class RabbitEncryptedConfig(
    var host: String,
    var port: Int = 5672,
    var virtualHost: String? = null,
    var username: String? = null,
    var password: String? = null,
) : EncryptedProducerConfig {
    override fun updateConfiguration(config: Configurable): RabbitEncryptedConfig {
        if (config is RabbitEncryptedConfig) {
            this.virtualHost = config.virtualHost
            this.port = config.port
            this.host = config.host
            this.username = config.username
            this.password = config.password
        }
        return this
    }
}