package paladin.router.models.configuration.brokers.core


import paladin.router.enums.configuration.Broker.BrokerType
import paladin.router.util.Configurable
import java.io.Serializable

sealed interface ProducerConfig : Serializable, Configurable {
    val brokerType: BrokerType
    var allowAsync: Boolean
    var retryMaxAttempts: Int
    var retryBackoff: Long
    var connectionTimeout: Long
    var errorHandlerStrategy: ErrorStrategy
    override fun updateConfiguration(config: Configurable): Configurable {
        if (config is ProducerConfig) {
            this.allowAsync = config.allowAsync
            this.retryMaxAttempts = config.retryMaxAttempts
            this.retryBackoff = config.retryBackoff
            this.connectionTimeout = config.connectionTimeout
        }
        return this
    }

    enum class ErrorStrategy {
        DLQ,
        AUDIT,
        CIRCUIT_BREAKER,
        IGNORE
    }
}