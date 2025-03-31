package paladin.router.enums.configuration

class Broker{
    enum class BrokerType {
        KAFKA,
        RABBIT,
        SQS,
        PULSAR,
        MQTT
    }

    enum class BrokerFormat {
        JSON,
        AVRO,
        PROTOBUF
    }

    companion object Factory{
        fun fromAvro(value: paladin.avro.database.BrokerFormat): BrokerFormat{
            return when(value){
                paladin.avro.database.BrokerFormat.JSON -> BrokerFormat.JSON
                paladin.avro.database.BrokerFormat.AVRO -> BrokerFormat.AVRO
                paladin.avro.database.BrokerFormat.PROTOBUF -> BrokerFormat.PROTOBUF
            }
        }

        fun fromAvro(value: paladin.avro.database.BrokerType): BrokerType{
            return when(value){
                paladin.avro.database.BrokerType.KAFKA -> BrokerType.KAFKA
                paladin.avro.database.BrokerType.RABBITMQ -> BrokerType.RABBIT
                paladin.avro.database.BrokerType.SQS -> BrokerType.SQS
                paladin.avro.database.BrokerType.PULSAR -> BrokerType.PULSAR
                // TODO: FIX OOPSIES
                else -> BrokerType.MQTT
            }
        }
    }
}

