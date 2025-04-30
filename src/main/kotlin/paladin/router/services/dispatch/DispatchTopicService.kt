package paladin.router.services.dispatch

import org.springframework.stereotype.Service
import paladin.router.models.dispatch.DispatchTopic
import paladin.router.models.dispatch.MessageDispatcher
import java.util.concurrent.ConcurrentHashMap

@Service
class DispatchTopicService {
    private val dispatcherTopics = ConcurrentHashMap<String, ConcurrentHashMap<MessageDispatcher, DispatchTopic>>()

    fun addDispatcherTopic(dispatcher: MessageDispatcher, topic: DispatchTopic) {
        dispatcherTopics.computeIfAbsent(topic.sourceTopic) { ConcurrentHashMap() }[dispatcher] = topic
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

    fun getDispatchTopic(topic: String, dispatcher: MessageDispatcher): DispatchTopic? {
        return dispatcherTopics[topic]?.get(dispatcher)
    }
}

