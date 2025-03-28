package paladin.router.enums.configuration

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