package paladin.router.models.configuration.brokers.core

import paladin.router.enums.configuration.Broker
import paladin.router.util.Configurable

data class KafkaProducerConfig(
    override val brokerType: Broker.BrokerType = Broker.BrokerType.KAFKA,
    val clientId: String,
    val groupId: String?,
    var enableAutoCommit: Boolean = false,
    var autoCommitIntervalMs: Int = 5000,
    var requestTimeoutMs: Int = 30000,
    var retries: Int = 5,
    var acks: String = "all",
) : ProducerConfig {
    override fun updateConfiguration(config: Configurable): KafkaProducerConfig {
        if (config is KafkaProducerConfig) {
            this.enableAutoCommit = config.enableAutoCommit
            this.autoCommitIntervalMs = config.autoCommitIntervalMs
            this.requestTimeoutMs = config.requestTimeoutMs
            this.retries = config.retries
            this.acks = config.acks
        }
        return this
    }
}