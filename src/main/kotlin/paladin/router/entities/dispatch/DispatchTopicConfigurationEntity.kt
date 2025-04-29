package paladin.router.entities.dispatch

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import jakarta.persistence.*
import java.util.*

@JsonIgnoreProperties(ignoreUnknown = true)
@Entity
@Table(
    name = "dispatch_topic", schema = "event_routing",
    indexes = [
        Index(name = "idx_dispatch_topic_name", columnList = "topic_name"),
    ]
)
data class DispatchTopicConfigurationEntity(
    @Id
    @GeneratedValue
    @Column(name = "id", columnDefinition = "UUID DEFAULT uuid_generate_v4()", nullable = false)
    val id: UUID? = null,

    @Column(name = "topic_name", nullable = false)
    var topicName: String,

    @Column(name = "key_format", nullable = true)
    @Enumerated(EnumType.STRING)

    val keyFormat: String? = null,
    @Column(name = "key_schema", nullable = true)

    val keySchema: String? = null,
    @Column(name = "value_format", nullable = false)

    @Enumerated(EnumType.STRING)
    val valueFormat: String,

    @Column(name = "value_schema", nullable = true)
    val valueSchema: String? = null,

    )