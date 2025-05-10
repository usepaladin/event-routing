package paladin.router.models.listener

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import java.io.Serializable

@JsonIgnoreProperties(ignoreUnknown = true)
data class AdditionalConsumerProperties(
    val schemaRegistryUrl: String? = null,
    val autoOffsetReset: String? = null,
    val enableAutoCommit: Boolean? = null,
    val maxPollRecords: Int? = null,
    val maxPollIntervalMs: Int? = null,
    val sessionTimeoutMs: Int? = null,
    val requestTimeoutMs: Int? = null,
    val autoCommitIntervalMs: Int? = null,
) : Serializable