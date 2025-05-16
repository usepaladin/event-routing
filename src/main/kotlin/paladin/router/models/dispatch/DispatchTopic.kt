package paladin.router.models.dispatch

import paladin.router.enums.configuration.Broker
import java.util.*

data class DispatchTopic(
    var id: UUID? = null,
    val sourceTopic: String,
    val destinationTopic: String,
    val key: Broker.ProducerFormat,
    val keySchema: String? = null,
    val value: Broker.ProducerFormat,
    val valueSchema: String? = null
) {
    companion object Factory {
        fun fromRequest(request: DispatchTopicRequest): DispatchTopic {
            return DispatchTopic(
                sourceTopic = request.sourceTopic,
                destinationTopic = request.destinationTopic,
                key = request.key,
                keySchema = request.keySchema,
                value = request.value,
                valueSchema = request.valueSchema
            )
        }
    }
}