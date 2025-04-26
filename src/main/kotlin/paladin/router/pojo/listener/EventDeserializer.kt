package paladin.router.pojo.listener

interface EventDeserializer<K, V> {
    fun deserializeKey(payload:ByteArray): K
    fun deserializeValue(payload:ByteArray): V
}