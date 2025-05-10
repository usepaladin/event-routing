package paladin.router.services.dispatch

import io.github.oshai.kotlinlogging.KLogger
import kotlinx.coroutines.*
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Service
import paladin.router.exceptions.BrokerNotFoundException
import paladin.router.models.dispatch.DispatchTopic
import paladin.router.models.dispatch.DispatchTopicRequest
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
    private val logger: KLogger,
    @Qualifier("coroutineDispatcher") private val dispatcher: CoroutineDispatcher
) : CoroutineScope {
    private val clientBrokers = ConcurrentHashMap<String, MessageDispatcher>()
    val job = SupervisorJob()
    override val coroutineContext: CoroutineContext
        get() = dispatcher + job

    fun <K, V> dispatchEvents(key: K, value: V, listener: EventListener) {
        launch {
            val dispatchTopics: ConcurrentHashMap<MessageDispatcher, DispatchTopic> =
                topicService.getDispatchersForTopic(listener.topic)
                    ?: throw IllegalStateException("Dispatch Service => No dispatchers found for topic: ${listener.topic}")

            dispatchTopics.asIterable().map {
                async {
                    val (dispatcher, topic) = it
                    try {
                        logger.info { "Dispatch Service => Dispatching event to dispatcher: ${dispatcher.broker.brokerName} => Topic: ${topic.destinationTopic} => Key: $key" }
                        dispatchToBroker(key, value, topic, dispatcher)
                    } catch (e: Exception) {
                        logger.error(e) { "Dispatch Service => Error dispatching event => Broker : ${dispatcher.broker.brokerType} - ${dispatcher.broker.brokerName} => Message: ${e.message}" }
                        // Todo: Handle DLQ logic here
                    }
                }
            }.awaitAll()
        }
    }

    private suspend fun <K, V> dispatchToBroker(key: K, value: V, topic: DispatchTopic, dispatcher: MessageDispatcher) {
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


    fun setDispatcher(brokerName: String, dispatcher: MessageDispatcher) {
        clientBrokers[brokerName] = dispatcher
    }

    fun removeDispatcher(brokerName: String) {
        clientBrokers.remove(brokerName)
    }

    fun getDispatcher(brokerName: String): MessageDispatcher? {
        return clientBrokers[brokerName]
    }

    fun getAllDispatchers(): List<MessageDispatcher> {
        return clientBrokers.values.toList()
    }

    fun addDispatcherTopic(dispatcherTopic: DispatchTopicRequest): DispatchTopic {
        clientBrokers[dispatcherTopic.dispatcher].let {
            if (it == null) {
                throw BrokerNotFoundException("Dispatcher for topic ${dispatcherTopic.dispatcher} not found")
            }


            return topicService.addDispatcherTopic(
                it,
                DispatchTopic.fromRequest(dispatcherTopic)
            )
        }
    }

    fun removeDispatcherFromTopic(topic: String, dispatcher: String): Unit {
        clientBrokers[dispatcher].let {
            if (it == null) {
                throw BrokerNotFoundException("Dispatcher for topic $topic not found")
            }
            topicService.removeDispatcherFromTopic(topic, it)
        }
    }

    fun editDispatcherForTopic(topic: DispatchTopicRequest): DispatchTopic {
        clientBrokers[topic.dispatcher].let {
            if (it == null) {
                throw BrokerNotFoundException("Dispatcher for topic ${topic.dispatcher} not found")
            }
            return topicService.updateDispatcherTopic(it, DispatchTopic.fromRequest(topic))
        }
    }

    fun removeSourceTopic(topic: String): Unit {
        topicService.removeTopic(topic)
    }

    fun getDispatchersOnTopic(topic: String): List<DispatchTopic> {
        return topicService.getDispatchersOnTopic(topic)
    }

    fun getTopicsForDispatcher(dispatcher: String): List<DispatchTopic> {
        clientBrokers[dispatcher].let {
            if (it == null) {
                throw BrokerNotFoundException("Dispatcher for topic $dispatcher not found")
            }
            return topicService.getAllTopicsForDispatcher(it)
        }
    }

    fun getAllTopics(): List<DispatchTopic> {
        return topicService.getAllTopics()
    }
}