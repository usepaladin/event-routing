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
        fun fromAvro(value: paladin.router.models.avro.database.BrokerFormat): BrokerFormat{
            return when(value){
                paladin.router.models.avro.database.BrokerFormat.JSON -> BrokerFormat.JSON
                paladin.router.models.avro.database.BrokerFormat.AVRO -> BrokerFormat.AVRO
                paladin.router.models.avro.database.BrokerFormat.PROTOBUF -> BrokerFormat.PROTOBUF
            }
        }

        fun fromAvro(value: paladin.router.models.avro.database.BrokerType): BrokerType{
            return when(value){
                paladin.router.models.avro.database.BrokerType.KAFKA -> BrokerType.KAFKA
                paladin.router.models.avro.database.BrokerType.RABBITMQ -> BrokerType.RABBIT
                paladin.router.models.avro.database.BrokerType.SQS -> BrokerType.SQS
                paladin.router.models.avro.database.BrokerType.PULSAR -> BrokerType.PULSAR
                // TODO: FIX OOPSIES
                else -> BrokerType.MQTT
            }
        }
    }
}

