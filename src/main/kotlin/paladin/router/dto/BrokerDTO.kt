package paladin.router.dto

import paladin.router.models.configuration.producers.MessageProducer
import paladin.router.models.configuration.producers.auth.EncryptedProducerConfig
import paladin.router.models.configuration.producers.core.ProducerConfig

open class ProducerDTO(
    val producer: MessageProducer,
    val config: ProducerConfig,
    val connectionConfig: EncryptedProducerConfig,
) {
    companion object Factory {
        fun fromEntity(producer: MessageProducer, config: ProducerConfig, connectionConfig: EncryptedProducerConfig) =
            ProducerDTO(
                producer = producer,
                config = config,
                connectionConfig = connectionConfig,
            )
    }
}