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
                        logger.info { "Dispatch Service => Dispatching event via ${dispatcher.identifier()} => Topic: ${topic.destinationTopic} => Key: $key" }
                        dispatchToBroker(key, value, topic, dispatcher)
                    } catch (e: Exception) {
                        logger.error(e) { "Dispatch Service => Error dispatching event => ${dispatcher.identifier()} => Message: ${e.message}" }
                        // Todo: Handle DLQ logic here
                    }
                }
            }.awaitAll()
        }
    }

    private suspend fun <K, V> dispatchToBroker(key: K, value: V, topic: DispatchTopic, dispatcher: MessageDispatcher) {
        repeat(
            dispatcher.producerConfig.retryMaxAttempts
        ) { retryAttempt ->
            if (dispatcher.connectionState.value == MessageDispatcher.MessageDispatcherState.Connected) {

                try {
                    dispatcher.dispatch(key, value, topic)
                    return
                } catch (ex: Exception) {
                    logger.warn { "Dispatch Service => ${dispatcher.identifier()} => Retry Attempt $retryAttempt/\$${dispatcher.producerConfig.retryMaxAttempts} => Error occurred during dispatch => ${ex.message}" }
                }

            } else {
                logger.warn { "Dispatch Service => ${dispatcher.identifier()} => Retry attempt $retryAttempt/\$${dispatcher.producerConfig.retryMaxAttempts} => Dispatcher is not connected at time of event production" }
            }


            delay(dispatcher.producerConfig.retryBackoff * (2F).pow(retryAttempt).toLong())
        }


        // Max retries exhausted, throw exception and send to DLQ for manual handling
        throw IOException("Failed to dispatch event after ${dispatcher.producerConfig.retryMaxAttempts} attempts")
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