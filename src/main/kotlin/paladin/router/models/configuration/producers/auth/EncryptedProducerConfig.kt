package paladin.router.models.configuration.producers.auth

import paladin.router.util.Configurable
import java.io.Serializable

interface EncryptedProducerConfig : Serializable, Configurable {
    override fun updateConfiguration(config: Configurable): Configurable
}