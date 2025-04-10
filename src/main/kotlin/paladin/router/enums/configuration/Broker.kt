package paladin.router.enums.configuration

class Broker{
    enum class BrokerType {
        KAFKA,
        RABBIT,
        SQS,
    }

    enum class BrokerFormat {
        STRING,
        JSON,
        AVRO,
    }

    companion object Factory{
        fun fromAvro(value: paladin.avro.database.BrokerFormat): BrokerFormat{
            return when(value){
                paladin.avro.database.BrokerFormat.STRING -> BrokerFormat.STRING
                paladin.avro.database.BrokerFormat.JSON -> BrokerFormat.JSON
                paladin.avro.database.BrokerFormat.AVRO -> BrokerFormat.AVRO
            }
        }

        fun fromAvro(value: paladin.avro.database.BrokerType): BrokerType{
            return when(value){
                paladin.avro.database.BrokerType.KAFKA -> BrokerType.KAFKA
                paladin.avro.database.BrokerType.RABBITMQ -> BrokerType.RABBIT
                paladin.avro.database.BrokerType.SQS -> BrokerType.SQS
            }
        }
    }
}

