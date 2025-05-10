package paladin.router.util

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode

fun JsonNode.toLinkedHashMap(): LinkedHashMap<String, Any?> {
    val map = LinkedHashMap<String, Any?>()
    this.fields().forEach { entry ->
        val key = entry.key
        val value = entry.value
        map[key] = when (value) {
            is ObjectNode -> value.toLinkedHashMap()
            else -> value
        }
    }
    return map
}