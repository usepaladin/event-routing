package paladin.router.pojo.configuration.brokers.auth

import paladin.router.util.factory.Configurable

data class RabbitEncryptedConfig (
    var addresses: String? = null,
): EncryptedBrokerConfig{
    override fun updateConfiguration(config: Configurable): RabbitEncryptedConfig {
        if (config is RabbitEncryptedConfig) {
            this.addresses = config.addresses
        }
        return this
    }
}