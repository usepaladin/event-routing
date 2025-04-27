package paladin.router.models.configuration.brokers.auth

import paladin.router.util.factory.Configurable
import java.io.Serializable

interface EncryptedBrokerConfig:Serializable, Configurable{
    override fun updateConfiguration(config: Configurable): Configurable
}