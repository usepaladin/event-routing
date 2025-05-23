package paladin.router.models.dispatch

import paladin.router.entities.dispatch.DispatchTopicConfigurationEntity
import paladin.router.enums.configuration.Broker
import java.util.*

data class DispatchTopic(
    var id: UUID? = null,
    val producerId: UUID,
    val sourceTopic: String,
    val destinationTopic: String,
    val key: Broker.ProducerFormat,
    val keySchema: String? = null,
    val value: Broker.ProducerFormat,
    val valueSchema: String? = null
) {
    companion object Factory {
        fun fromRequest(request: DispatchTopicRequest, producerId: UUID): DispatchTopic {
            return DispatchTopic(
                producerId = producerId,
                sourceTopic = request.sourceTopic,
                destinationTopic = request.destinationTopic,
                key = request.key,
                keySchema = request.keySchema,
                value = request.value,
                valueSchema = request.valueSchema
            )
        }

        fun fromEntity(entity: DispatchTopicConfigurationEntity): DispatchTopic {
            return DispatchTopic(
                id = entity.id,
                producerId = entity.producerId,
                sourceTopic = entity.sourceTopic,
                destinationTopic = entity.destinationTopic,
                key = entity.keyFormat ?: Broker.ProducerFormat.STRING,
                keySchema = entity.keySchema,
                value = entity.valueFormat,
                valueSchema = entity.valueSchema
            )
        }
    }
}