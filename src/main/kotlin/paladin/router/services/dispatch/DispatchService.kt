package paladin.router.services.dispatch

import io.github.oshai.kotlinlogging.KLogger
import org.apache.avro.specific.SpecificRecord
import org.springframework.stereotype.Service
import paladin.router.enums.configuration.Broker
import paladin.router.pojo.dispatch.DispatchEvent
import paladin.router.pojo.dispatch.MessageDispatcher
import java.io.IOException
import java.util.concurrent.ConcurrentHashMap
import kotlin.math.pow

@Service
class DispatchService(private val logger: KLogger) {
    private val clientBrokers = ConcurrentHashMap<String, MessageDispatcher>()
    private final val MAX_RETRY_ATTEMPTS: Int = 3
    private final val MIN_RETRY_BACKOFF: Long = 1000L // 1 second

    fun <T: SpecificRecord> dispatchEvent(event: DispatchEvent<T>){
        try{
            val dispatcher: MessageDispatcher = clientBrokers[event.brokerName]
                ?: throw IOException("No dispatcher found for broker: ${event.brokerName}")

            // Ensure Broker is of expected type
            if(event.brokerType != dispatcher.broker.brokerType){
                throw IOException("Broker type mismatch: ${event.brokerType} != ${dispatcher.broker.brokerType}")
            }

            // Ensure Formats are of expected type to avoid serialization issues
            if(event.keyFormat != dispatcher.broker.keySerializationFormat){
                throw IOException("Broker format mismatch: ${event.keyFormat} != ${dispatcher.broker.keySerializationFormat}")
            }

            if(event.payloadFormat != dispatcher.broker.valueSerializationFormat){
                throw IOException("Broker format mismatch: ${event.payloadFormat} != ${dispatcher.broker.valueSerializationFormat}")
            }

            //Todo - Switch to Retry Template

            // If the dispatcher is not connected, we will retry the dispatch for a short period of time
            var retryAttempt = 0
            while (retryAttempt < MAX_RETRY_ATTEMPTS) {
                try {
                    Thread.sleep(MIN_RETRY_BACKOFF * (2F).pow(retryAttempt).toLong())
                    if(dispatcher.connectionState.value == MessageDispatcher.MessageDispatcherState.Connected){

                        if(dispatcher.broker.keySerializationFormat == null){
                            dispatcher.dispatch(event.topic, event.payload, event.payloadSchema)
                        } else {
                            dispatcher.dispatch(event.topic, event.payload, event.keySchema, event.payloadSchema)
                        }
                        return
                    }
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()
                    throw e
                }
                retryAttempt++
            }

            // Connection failed after retry attempts, send to DLQ
            logger.error { "Dispatch Service => Failed to dispatch event after $MAX_RETRY_ATTEMPTS attempts" }
            logger.info { "Dispatch Service => Sending event to DLQ" }
            TODO()
        }
        catch (e: Exception){
            logger.error(e) { "Dispatch Service => Error dispatching event => ${e.message}" }
            throw e
        }
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