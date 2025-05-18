package paladin.router.models.dispatch

import io.github.oshai.kotlinlogging.KLogger
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import paladin.router.models.configuration.producers.MessageProducer
import paladin.router.models.configuration.producers.auth.EncryptedProducerConfig
import paladin.router.models.configuration.producers.core.ProducerConfig
import paladin.router.services.schema.SchemaService
import java.io.Serializable

abstract class MessageDispatcher : Serializable, AutoCloseable {
    abstract val schemaService: SchemaService
    abstract val producer: MessageProducer
    abstract val producerConfig: ProducerConfig
    abstract val connectionConfig: EncryptedProducerConfig
    abstract val logger: KLogger
    abstract val meterRegistry: MeterRegistry?

    private val _connectionState = MutableStateFlow<MessageDispatcherState>(MessageDispatcherState.Disconnected)

    fun getConnectionState(): MessageDispatcherState {
        return _connectionState.value
    }

    val connectionState: StateFlow<MessageDispatcherState> = _connectionState


    fun updateConnectionState(state: MessageDispatcherState) {
        _connectionState.value = state
    }

    abstract fun <V> dispatch(payload: V, topic: DispatchTopic)

    /**
     * Uses the provided producer to dispatch a message to the appropriate destination within the brokers reach.
     */
    abstract fun <K, V> dispatch(key: K, payload: V, topic: DispatchTopic)

    /**
     * Builds the dispatcher of the message producer from the configuration properties
     * provided by the user
     */
    abstract fun build()
    abstract fun testConnection()

    /**
     * Validates the dispatcher of the message producer,
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

    fun identifier(): String {
        return "${producer.brokerType} Producer => Producer Name: ${name()}"
    }

    fun name(): String {
        return producer.producerName
    }
}