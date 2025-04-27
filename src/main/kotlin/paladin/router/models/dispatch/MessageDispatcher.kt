package paladin.router.models.dispatch

import io.github.oshai.kotlinlogging.KLogger
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import paladin.router.models.configuration.brokers.MessageBroker
import paladin.router.models.configuration.brokers.core.BrokerConfig
import paladin.router.models.configuration.brokers.auth.EncryptedBrokerConfig
import paladin.router.services.schema.SchemaService
import java.io.Serializable

abstract class MessageDispatcher: Serializable{
    abstract val schemaService: SchemaService
    abstract val broker: MessageBroker
    abstract val config: BrokerConfig
    abstract val authConfig: EncryptedBrokerConfig
    abstract val logger: KLogger

    private val _connectionState = MutableStateFlow<MessageDispatcherState>(MessageDispatcherState.Disconnected)

    fun getConnectionState(): MessageDispatcherState {
        return _connectionState.value
    }

    val connectionState: StateFlow<MessageDispatcherState> = _connectionState


    fun updateConnectionState(state: MessageDispatcherState) {
        _connectionState.value = state
    }

    /**
     * Uses the provided broker to dispatch a message to the appropriate destination within the brokers reach.
     */
    abstract fun <K, V> dispatch(key: K, payload: V, topic: DispatchTopic)

    /**
     * Builds the dispatcher of the message broker from the configuration properties
     * provided by the user
     */
    abstract fun build()
    abstract fun testConnection()

    /**
     * Validates the dispatcher of the message broker,
     * ensuring all configuration properties required for successful operation are present
     * before the dispatcher is built and saved for use.
     */
    abstract fun validate()

    sealed class MessageDispatcherState {
        data object Disconnected : MessageDispatcherState()
        data object Building : MessageDispatcherState()
        data object Connected : MessageDispatcherState()
        data class Error(val exception: Throwable) : MessageDispatcherState()
    }
}