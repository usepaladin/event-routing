package paladin.router.configuration

import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class LoggerConfig {

    @Bean
    fun logger(): KLogger {
        return KotlinLogging.logger {}
    }
}