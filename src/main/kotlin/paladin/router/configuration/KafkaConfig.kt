package paladin.router.configuration

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.KafkaListenerEndpointRegistry

@EnableKafka
@Configuration
class KafkaConfig{
    @Bean
    fun kafkaListenerEndpointRegistry(): KafkaListenerEndpointRegistry{
        return KafkaListenerEndpointRegistry()
    }
}
