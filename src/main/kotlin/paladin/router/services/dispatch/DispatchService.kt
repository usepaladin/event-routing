package paladin.router.services.dispatch

import io.github.oshai.kotlinlogging.KLogger
import kotlinx.coroutines.*
import org.apache.avro.specific.SpecificRecord
import org.springframework.stereotype.Service
import paladin.router.dto.MessageDispatchDTO
import paladin.router.enums.configuration.Broker
import paladin.router.pojo.dispatch.DispatchEvent
import paladin.router.pojo.dispatch.MessageDispatcher
import paladin.router.pojo.listener.EventListener
import java.io.IOException
import java.util.concurrent.ConcurrentHashMap
import kotlin.coroutines.CoroutineContext
import kotlin.math.pow

private const val MAX_RETRY_ATTEMPTS: Int = 3
private const val MIN_RETRY_BACKOFF: Long = 1000L // 1 second

@Service
class DispatchService(private val logger: KLogger): CoroutineScope {
    private val clientBrokers = ConcurrentHashMap<String, MessageDispatcher>()
    override val coroutineContext: CoroutineContext
        get() = Dispatchers.IO + SupervisorJob()
    
    fun <T> dispatchEvents(payload: T, listener: EventListener, dispatchers: List<MessageDispatcher>) = launch {
        dispatchers.map { dispatcher ->
            async {
                try{
                    dispatchToBroker(payload, dispatcher)
                } catch (e: Exception){
                    logger.error(e) { "Dispatch Service => Error dispatching event => Broker : ${event.brokerType} => ${event.brokerName} => ${event.topic} => Message: ${e.message}" }
                    // Send to DLQ For manual handling
                }
            }
        }.awaitAll()
    }

    private suspend fun <T> dispatchToBroker(event: T, dispatcher: MessageDispatcher){
            // If the dispatcher is not connected, we will retry the dispatch for a short period of time before sending to DLQ
            repeat(
                MAX_RETRY_ATTEMPTS
            ) { retryAttempt ->
                if (dispatcher.connectionState.value == MessageDispatcher.MessageDispatcherState.Connected) {
                    if(dispatcher.broker.keySerializationFormat == null){
                        dispatcher.dispatch(event.topic, event.payload, event.payloadSchema)
                    } else {
                        dispatcher.dispatch(event.topic, "key", event.payload, event.keySchema, event.payloadSchema)
                    }
                    return
                }

                logger.warn { "Dispatch Service => Retrying dispatch to ${event.brokerName} => Attempt: ${retryAttempt + 1}" }
                delay(MIN_RETRY_BACKOFF * (2F).pow(retryAttempt).toLong())
            }

            // Max retries exhausted, throw exception and send to DLQ for manual handling
            logger.error { "Dispatch Service => Failed to dispatch event after $MAX_RETRY_ATTEMPTS attempts" }
            throw IOException("Failed to dispatch event after $MAX_RETRY_ATTEMPTS attempts")
    }


    fun setDispatcher(brokerName: String, dispatcher: MessageDispatcher){
        clientBrokers[brokerName] = dispatcher
    }

    fun removeDispatcher(brokerName: String){
        clientBrokers.remove(brokerName)
    }

    fun getDispatcher(brokerName: String): MessageDispatcher? {
        return clientBrokers[brokerName]
    }

    fun getAllDispatchers(): List<MessageDispatcher> {
        return clientBrokers.values.toList()
    }

}