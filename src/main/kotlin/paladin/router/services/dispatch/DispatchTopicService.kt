package paladin.router.services.dispatch

import org.springframework.stereotype.Service
import paladin.router.models.dispatch.DispatchTopic
import paladin.router.models.dispatch.MessageDispatcher
import java.util.concurrent.ConcurrentHashMap

@Service
class DispatchTopicService {
    private val dispatcherTopics = ConcurrentHashMap<String, ConcurrentHashMap<MessageDispatcher, DispatchTopic>>()

    fun addDispatcherTopic(dispatcher: MessageDispatcher, topic: DispatchTopic): DispatchTopic {
        dispatcherTopics.computeIfAbsent(topic.sourceTopic) { ConcurrentHashMap() }[dispatcher] = topic
        return topic
    }

    fun updateDispatcherTopic(dispatcher: MessageDispatcher, topic: DispatchTopic): DispatchTopic {
        dispatcherTopics[topic.sourceTopic]?.let { topics ->
            if (topics.containsKey(dispatcher)) {
                topics[dispatcher] = topic
            }
        }
        return topic
    }

    fun getDispatchersForTopic(topic: String): ConcurrentHashMap<MessageDispatcher, DispatchTopic>? {
        return dispatcherTopics[topic]
    }

    fun removeTopic(topic: String) {
        dispatcherTopics.remove(topic)
    }

    fun removeDispatcherFromTopic(topic: String, dispatcher: MessageDispatcher) {
        dispatcherTopics[topic]?.remove(dispatcher)
    }

    fun getDispatchersOnTopic(topic: String): List<DispatchTopic> {
        return dispatcherTopics[topic].let {
            if (it.isNullOrEmpty()) {
                return emptyList()
            }

            it.values.toList()
        }
    }

    fun getAllTopicsForDispatcher(dispatcher: MessageDispatcher): List<DispatchTopic> {
        return dispatcherTopics.values.flatMap { sourceTopic ->
            sourceTopic[dispatcher]?.let { listOf(it) } ?: emptyList()
        }
    }

    fun getAllTopics(): List<DispatchTopic> {
        return dispatcherTopics.values.flatMap { it.values }
    }
}

