package paladin.router.util

import kotlin.reflect.full.memberProperties

fun fromDataclassToMap(obj: Any): Map<String, Any?> {
    val map = mutableMapOf<String, Any?>()
    val kClass = obj::class
    for (property in kClass.memberProperties) {
        val value = property.getter.call(obj)
        map[property.name] = value
    }
    return map
}

fun fromDataclassToMap(objs: List<Any>): Map<String, Any> {
    val map = mutableMapOf<String, Any>()
    for (obj in objs) {
        val kClass = obj::class
        for (property in kClass.memberProperties) {
            val value = property.getter.call(obj)
            if (value != null) {
                map[property.name] = value.let {
                    if (it is Enum<*>) return@let it.name
                    it
                }
            }
        }
    }
    return map
}