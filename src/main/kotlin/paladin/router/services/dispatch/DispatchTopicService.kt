package paladin.router.services.dispatch

import org.springframework.stereotype.Service
import paladin.router.models.dispatch.DispatchTopic
import paladin.router.models.dispatch.MessageDispatcher
import java.util.concurrent.ConcurrentHashMap

@Service
class DispatchTopicService {
    private val dispatcherTopics = ConcurrentHashMap<String, ConcurrentHashMap<MessageDispatcher, DispatchTopic>>()

    fun addDispatcherTopicToTopic(topic: String, dispatcher: MessageDispatcher, dispatchTopic: DispatchTopic) {
        dispatcherTopics.computeIfAbsent(topic) { ConcurrentHashMap() }[dispatcher] = dispatchTopic
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

