package paladin.router.models.configuration.brokers.core


import paladin.router.enums.configuration.Broker.BrokerType
import paladin.router.util.factory.Configurable
import java.io.Serializable

sealed interface BrokerConfig : Serializable, Configurable {
    val brokerType: BrokerType
    override fun updateConfiguration(config: Configurable): Configurable
}