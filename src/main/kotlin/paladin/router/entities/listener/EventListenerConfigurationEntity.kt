package paladin.router.entities.listener

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import io.hypersistence.utils.hibernate.type.array.StringArrayType
import jakarta.persistence.*
import org.hibernate.annotations.Type
import paladin.router.enums.configuration.Broker
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

    @Column(name = "key_format", nullable = true)
    @Enumerated(EnumType.STRING)
    val keyFormat: Broker.BrokerFormat?,

    @Column(name = "value_format", nullable = false)
    @Enumerated(EnumType.STRING)
    val valueFormat: Broker.BrokerFormat,
    
    @Column(name = "created_at", columnDefinition = "TIMESTAMP DEFAULT CURRENT_TIMESTAMP", updatable = false)
    var createdAt: String = "CURRENT_TIMESTAMP",

    @Column(name = "updated_at", columnDefinition = "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
    var updatedAt: String = "CURRENT_TIMESTAMP"
)