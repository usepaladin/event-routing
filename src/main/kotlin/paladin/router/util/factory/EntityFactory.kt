package paladin.router.util.factory

import paladin.router.entities.dispatch.DispatchTopicConfigurationEntity
import paladin.router.entities.listener.EventListenerConfigurationEntity
import paladin.router.models.dispatch.DispatchTopic
import paladin.router.models.listener.EventListener


fun EventListener.toEntity(): EventListenerConfigurationEntity {
    return EventListenerConfigurationEntity(
        id = this.id,
        topic = this.topic,
        groupId = this.groupId,
        runOnStartup = this.runOnStartup,
        keyFormat = this.key,
        valueFormat = this.value,
        dispatchers = this.dispatchers.map { it.broker.brokerName }
    )
}

fun DispatchTopic.toEntity(): DispatchTopicConfigurationEntity {
    return DispatchTopicConfigurationEntity(
        id = this.id,
        sourceTopic = this.sourceTopic,
        destinationTopic = this.destinationTopic,
        keyFormat = this.key,
        keySchema = this.keySchema,
        valueFormat = this.value,
        valueSchema = this.valueSchema
    )
}
