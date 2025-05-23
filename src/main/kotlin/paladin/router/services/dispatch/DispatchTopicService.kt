package paladin.router.services.dispatch

import org.springframework.stereotype.Service
import paladin.router.models.dispatch.DispatchTopic
import paladin.router.models.dispatch.MessageDispatcher
import paladin.router.repository.DispatchTopicRepository
import paladin.router.util.factory.EntityFactory.toEntity
import java.util.concurrent.ConcurrentHashMap

@Service
class DispatchTopicService(
    private val repository: DispatchTopicRepository
) {
    private val dispatcherTopics = ConcurrentHashMap<String, ConcurrentHashMap<MessageDispatcher, DispatchTopic>>()

    fun addDispatcherTopic(dispatcher: MessageDispatcher, topic: DispatchTopic): DispatchTopic {
        return try {
            val savedEntity = repository.save(topic.toEntity())
            topic.apply { id = savedEntity.id }
            dispatcherTopics.computeIfAbsent(topic.sourceTopic) { ConcurrentHashMap() }[dispatcher] = topic
            topic
        } catch (e: Exception) {
            // Log error and rethrow
            throw RuntimeException("Failed to add dispatcher topic", e)
        }
    }

    fun updateDispatcherTopic(dispatcher: MessageDispatcher, topic: DispatchTopic): DispatchTopic {
        repository.save(topic.toEntity())
        dispatcherTopics[topic.sourceTopic]?.let { topics ->
            if (topics.containsKey(dispatcher)) {
                topics[dispatcher] = topic
            }
        }
        return topic
    }

    fun init(dispatcher: MessageDispatcher) {
        try {
            repository.findByProducerId(dispatcher.producer.id).map {
                DispatchTopic.fromEntity(it).run {
                    dispatcherTopics.computeIfAbsent(this.sourceTopic) { ConcurrentHashMap() }[dispatcher] = this
                }
            }
        } catch (e: Exception) {
            throw RuntimeException("Failed to initialize dispatcher topics for producer ${dispatcher.producer.id}", e)
        }

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

