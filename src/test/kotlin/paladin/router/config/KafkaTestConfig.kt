package paladin.router.config

import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.utils.KafkaTestUtils

@TestConfiguration
class KafkaTestConfig {

    @Bean
    @Primary
    fun kafkaConsumerFactory(
        embeddedKafkaBroker: EmbeddedKafkaBroker,
        kafkaProperties: KafkaProperties
    ): DefaultKafkaConsumerFactory<Any, Any> {
        val consumerProps = KafkaTestUtils.consumerProps("test-group", "true", embeddedKafkaBroker)
        return DefaultKafkaConsumerFactory(consumerProps)
    }

    @Bean
    @Primary
    fun kafkaProducerFactory(embeddedKafkaBroker: EmbeddedKafkaBroker): DefaultKafkaProducerFactory<Any, Any> {
        val producerProps = KafkaTestUtils.producerProps(embeddedKafkaBroker)
        return DefaultKafkaProducerFactory(producerProps)
    }
}