package paladin.router.pojo.listener

interface EventListener<K,V> {
    val topic: String
    val group: String
    val deserializer: EventDeserializer<K, V>

    fun process(key:K, value: V)
    fun start()
    fun stop()
}