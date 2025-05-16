package paladin.router.models.configuration.brokers.core

import paladin.router.enums.configuration.Broker
import paladin.router.util.Configurable

data class KafkaProducerConfig(
    override val brokerType: Broker.BrokerType = Broker.BrokerType.KAFKA,
    val clientId: String,
    val groupId: String?,
    val enableSchemaRegistry: Boolean = true,
    // Synchronous or Asynchronous sending of messages
    var allowAsync: Boolean = true,
    // Enable auto commit of offsets
    var enableAutoCommit: Boolean = false,
    // Interval in milliseconds to commit offsets
    var autoCommitIntervalMs: Int = 5000,
    // Timeout in milliseconds for requests
    var requestTimeoutMs: Int = 30000,
    // Number of retries for sending messages
    var retries: Int = 5,
    // Acknowledgment level for message delivery
    var acks: String = "all",
    val batchSize: Int = 16384, // Default batch size
    val lingerMs: Int = 1, // Time to buffer messages
    val compressionType: CompressionType = CompressionType.NONE, // none, gzip, snappy, lz4
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

    enum class CompressionType(val type: String) {
        NONE("none"),
        GZIP("gzip"),
        SNAPPY("snappy"),
        LZ4("lz4"),
        ZSTD("zstd");
    }
}