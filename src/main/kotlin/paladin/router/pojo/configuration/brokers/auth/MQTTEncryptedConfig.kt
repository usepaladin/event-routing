package paladin.router.pojo.configuration.brokers.auth

import paladin.router.util.factory.Configurable

data class MQTTEncryptedConfig(
    var addresses: String? = null,
    var virtualHost: String? = null,
    var username: String? = null,
    var password: String? = null,
): EncryptedBrokerConfig{
    override fun updateConfiguration(config: Configurable): MQTTEncryptedConfig {
        if (config is MQTTEncryptedConfig) {
            this.addresses = config.addresses
            this.virtualHost = config.virtualHost
            this.username = config.username
            this.password = config.password
        }
        return this
    }
}