package paladin.router.configuration.properties

import jakarta.validation.constraints.NotBlank
import org.jetbrains.annotations.NotNull
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.validation.annotation.Validated
import java.util.*

@ConfigurationProperties(prefix = "router")
@Validated
data class CoreConfigurationProperties(
    @field:NotBlank
    val tenantId: String
)
