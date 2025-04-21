package paladin.router

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication
import org.springframework.kafka.annotation.EnableKafka

@EnableConfigurationProperties
@EnableKafka
@ConfigurationPropertiesScan
@SpringBootApplication
class EventRouterApplication

fun main(args: Array<String>) {
    runApplication<EventRouterApplication>(*args)
}
