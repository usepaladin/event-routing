package paladin.router.models.configuration.producers.core

import paladin.router.enums.configuration.Broker
import paladin.router.util.Configurable

data class KafkaProducerConfig(
    override val brokerType: Broker.BrokerType = Broker.BrokerType.KAFKA,
    val clientId: String,
    val groupId: String?,
    var enableSchemaRegistry: Boolean = false,
    // Synchronous or Asynchronous sending of messages
    override var allowAsync: Boolean = true,
    override var retryMaxAttempts: Int = 3,
    override var retryBackoff: Long = 1000L,
    override var connectionTimeout: Long = 10000L,
    override var errorHandlerStrategy: ProducerConfig.ErrorStrategy = ProducerConfig.ErrorStrategy.DLQ,
    // Enable auto commit of offsets
    var enableAutoCommit: Boolean = false,
    // Interval in milliseconds to commit offsets
    var autoCommitIntervalMs: Int = 5000,
    // Acknowledgment level for message delivery
    var acks: String = "all",
    var batchSize: Int = 16384, // Default batch size
    var lingerMs: Int = 1, // Time to buffer messages
    var compressionType: CompressionType = CompressionType.NONE, // none, gzip, snappy, lz4
) : ProducerConfig {
    override fun updateConfiguration(config: Configurable): KafkaProducerConfig {
        if (config is KafkaProducerConfig) {
            super.updateConfiguration(config)
            this.enableAutoCommit = config.enableAutoCommit
            this.autoCommitIntervalMs = config.autoCommitIntervalMs
            this.acks = config.acks
            this.batchSize = config.batchSize
            this.lingerMs = config.lingerMs
            this.compressionType = config.compressionType
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