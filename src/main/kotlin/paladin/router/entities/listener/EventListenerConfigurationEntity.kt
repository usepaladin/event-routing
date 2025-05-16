package paladin.router.entities.listener

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import io.hypersistence.utils.hibernate.type.json.JsonBinaryType
import jakarta.persistence.*
import org.hibernate.annotations.Type
import paladin.router.enums.configuration.Broker
import paladin.router.models.listener.AdditionalConsumerProperties
import java.time.ZonedDateTime
import java.util.*

@JsonIgnoreProperties(ignoreUnknown = true)
@Entity
@Table(
    name = "event_listener",
    schema = "event_routing",
    uniqueConstraints = [
        UniqueConstraint(columnNames = ["topic_name"])
    ],
    indexes = [
        Index(name = "idx_event_listener_topic_name", columnList = "topic_name"),
    ]
)
data class EventListenerConfigurationEntity(
    @Id
    @GeneratedValue
    @Column(name = "id", columnDefinition = "UUID DEFAULT uuid_generate_v4()", nullable = false)
    val id: UUID? = null,

    @Column(name = "topic_name", nullable = false)
    var topic: String,

    @Column(name = "group_id", nullable = false)
    var groupId: String,

    @Column(name = "run_on_startup", nullable = false)
    var runOnStartup: Boolean = false,

    @Column(name = "key_format", nullable = false)
    @Enumerated(EnumType.STRING)
    val keyFormat: Broker.ProducerFormat,

    @Column(name = "value_format", nullable = false)
    @Enumerated(EnumType.STRING)
    val valueFormat: Broker.ProducerFormat,

    @Type(JsonBinaryType::class)
    @Column(name = "additional_properties", columnDefinition = "jsonb")
    val consumerProperties: AdditionalConsumerProperties,

    @Column(name = "created_at", columnDefinition = "TIMESTAMP DEFAULT CURRENT_TIMESTAMP", updatable = false)
    var createdAt: ZonedDateTime = ZonedDateTime.now(),

    @Column(name = "updated_at", columnDefinition = "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
    var updatedAt: ZonedDateTime = ZonedDateTime.now()
)