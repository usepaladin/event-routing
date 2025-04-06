package paladin.router.pojo.configuration.brokers.auth

import paladin.router.util.factory.Configurable

data class PulsarEncryptedConfig(
    val temp: String? =null
): EncryptedBrokerConfig{
    override fun updateConfiguration(config: Configurable): PulsarEncryptedConfig {
        if (config is PulsarEncryptedConfig) {
            TODO()
        }
        return this
    }
}