package paladin.router.util.factory

import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.kafka.support.serializer.JsonSerializer
import paladin.router.enums.configuration.Broker


object SerializerFactory {
    fun fromFormat(format: Broker.BrokerFormat): String{
            return when(format){
                Broker.BrokerFormat.STRING -> StringSerializer::class.java.name
                Broker.BrokerFormat.JSON -> JsonSerializer::class.java.name
                Broker.BrokerFormat.AVRO -> KafkaAvroSerializer::class.java.name
            }
        }
}
