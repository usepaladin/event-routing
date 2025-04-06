package paladin.router.pojo.dispatch

import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import paladin.router.models.configuration.brokers.MessageBroker
import paladin.router.pojo.configuration.brokers.core.BrokerConfig
import paladin.router.pojo.configuration.brokers.auth.EncryptedBrokerConfig

abstract class MessageDispatcher{
    abstract val broker: MessageBroker
    abstract val config: BrokerConfig
    abstract val authConfig: EncryptedBrokerConfig

    private val _connectionState = MutableStateFlow<MessageDispatcherState>(MessageDispatcherState.Disconnected)
    val connectionState: StateFlow<MessageDispatcherState> = _connectionState

    fun updateConnectionSTate(state: MessageDispatcherState) {
        _connectionState.value = state
    }

    /**
     * Uses the provided broker to dispatch a message to the appropriate destination within the brokers reach.
     */
    abstract fun <K, V> dispatch(topic: String, key: K, payload: V, schema: String? = null)
    abstract fun <V> dispatch(topic: String, payload: V, schema: String? = null)
    
    /**
     * Builds the dispatcher of the message broker from the configuration properties
     * provided by the user
     */
    abstract fun build()

    /**
     * Validates the dispatcher of the message broker,
     * ensuring all configuration properties required for successful operation are present
     * before the dispatcher is built and saved for use.
     */
    abstract fun validate()

    sealed class MessageDispatcherState {
        data object Disconnected : MessageDispatcherState()
        data object Connecting : MessageDispatcherState()
        data object Connected : MessageDispatcherState()
        data object Error : MessageDispatcherState()
    }
}