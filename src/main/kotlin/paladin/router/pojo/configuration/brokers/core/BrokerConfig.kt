package paladin.router.pojo.configuration.brokers.core


import paladin.router.enums.configuration.Broker.BrokerType
import java.io.Serializable

sealed interface BrokerConfig : Serializable {
    val brokerType: BrokerType
}