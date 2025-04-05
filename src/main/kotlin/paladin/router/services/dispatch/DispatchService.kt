package paladin.router.services.dispatch

import io.github.oshai.kotlinlogging.KLogger
import org.apache.avro.specific.SpecificRecord
import org.springframework.stereotype.Service
import paladin.router.pojo.dispatch.DispatchEvent
import paladin.router.pojo.dispatch.MessageDispatcher
import java.io.IOException
import java.util.concurrent.ConcurrentHashMap

@Service
class DispatchService(private val logger: KLogger) {
    private val clientBrokers = ConcurrentHashMap<String, MessageDispatcher>()

    fun <T: SpecificRecord> dispatchEvent(event: DispatchEvent<T>){
        val dispatcher: MessageDispatcher? = clientBrokers[event.brokerName]

        if(dispatcher == null){
            logger.error { "Dispatcher not found for broker: ${event.brokerName}" }
            throw IOException("Dispatcher not found for broker: ${event.brokerName}")
        }

        // Ensure Broker is of expected type
        if(event.brokerType !== dispatcher.broker.brokerType){
            throw IOException("Broker type mismatch: ${event.brokerType} != ${dispatcher.broker.brokerType}")
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