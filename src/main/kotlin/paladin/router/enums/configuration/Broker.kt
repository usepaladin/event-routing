package paladin.router.enums.configuration

class Broker {
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
}

