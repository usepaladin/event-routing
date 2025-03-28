package paladin.router

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class EventRouterApplication

fun main(args: Array<String>) {
    runApplication<EventRouterApplication>(*args)
}
