package paladin.router.models.configuration.brokers.auth

import paladin.router.util.Configurable
import java.io.Serializable

interface EncryptedBrokerConfig : Serializable, Configurable {
    override fun updateConfiguration(config: Configurable): Configurable
}