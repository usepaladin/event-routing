package paladin.router.pojo.configuration.brokers.core

import paladin.router.enums.configuration.Broker

data class KafkaBrokerConfig(
    override val brokerType: Broker.BrokerType = Broker.BrokerType.KAFKA,
    val clientId: String,
    val groupId: String?,
    val enableAutoCommit: Boolean = false,
    val autoCommitIntervalMs: Int = 5000,
    val requestTimeoutMs: Int = 30000,
    val retries: Int = 5,
    val acks: String = "all",
) : BrokerConfig