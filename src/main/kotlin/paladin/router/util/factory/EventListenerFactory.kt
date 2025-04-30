package paladin.router.util.factory

import paladin.router.entities.listener.EventListenerConfigurationEntity
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
