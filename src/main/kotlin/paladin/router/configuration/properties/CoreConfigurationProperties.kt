package paladin.router.configuration.properties

import org.springframework.boot.context.properties.ConfigurationProperties
import java.util.*

@ConfigurationProperties(prefix = "router")
data class CoreConfigurationProperties(
    val serverInstanceId: UUID,
    val tenantId: String
)
