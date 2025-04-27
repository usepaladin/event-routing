package paladin.router.services.dispatch

import io.github.oshai.kotlinlogging.KLogger
import kotlinx.coroutines.*
import org.springframework.stereotype.Service
import paladin.router.models.dispatch.DispatchTopic
import paladin.router.models.dispatch.MessageDispatcher
import paladin.router.models.listener.EventListener
import java.io.IOException
import java.util.concurrent.ConcurrentHashMap
import kotlin.coroutines.CoroutineContext
import kotlin.math.pow

private const val MAX_RETRY_ATTEMPTS: Int = 3
private const val MIN_RETRY_BACKOFF: Long = 1000L // 1 second

@Service
class DispatchService(
    private val topicService: DispatchTopicService,
    private val logger: KLogger): CoroutineScope {
    private val clientBrokers = ConcurrentHashMap<String, MessageDispatcher>()
    override val coroutineContext: CoroutineContext
        get() = Dispatchers.IO + SupervisorJob()
    
    fun <K,V> dispatchEvents(key: K, value: V, listener: EventListener, dispatchers: List<MessageDispatcher>) = launch {
        val dispatchTopics: ConcurrentHashMap<MessageDispatcher, DispatchTopic> = topicService.getDispatchersForTopic(listener.topic)?:
            throw IllegalStateException("Dispatch Service => No dispatchers found for topic: ${listener.topic}")

        dispatchers.map { dispatcher ->
            async {
                try{
                    dispatchTopics[dispatcher].let { topic ->
                        if(topic == null){
                            logger.error { "Dispatch Service => No dispatch topic found for dispatcher: ${dispatcher.broker.brokerName} => Topic: ${listener.topic}" }
                            return@async
                        }
                        logger.info { "Dispatch Service => Dispatching event to dispatcher: ${dispatcher.broker.brokerName} => Topic: ${topic.topic} => Key: $key" }
                        dispatchToBroker(key, value, topic, dispatcher)
                    }
                } catch (e: Exception){
                    logger.error(e) { "Dispatch Service => Error dispatching event => Broker : ${dispatcher.broker.brokerType} - ${dispatcher.broker.brokerName} => Message: ${e.message}" }
                    // Send to DLQ For manual handling
                }
            }
        }.awaitAll()
    }

    private suspend fun <K,V> dispatchToBroker(key: K, value: V, topic: DispatchTopic, dispatcher: MessageDispatcher){
            // If the dispatcher is not connected, we will retry the dispatch for a short period of time before sending to DLQ
            repeat(
                MAX_RETRY_ATTEMPTS
            ) { retryAttempt ->
                if (dispatcher.connectionState.value == MessageDispatcher.MessageDispatcherState.Connected) {
                    dispatcher.dispatch(key, value, topic)
                    return
                }

                logger.warn { "Dispatch Service => Dispatcher Name: ${dispatcher.broker.brokerName} => Dispatcher is not connected at time of event production => Attempting Retry attempt $retryAttempt/$MAX_RETRY_ATTEMPTS" }
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