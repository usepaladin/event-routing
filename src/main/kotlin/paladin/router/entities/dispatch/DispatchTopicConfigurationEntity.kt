package paladin.router.entities.dispatch

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import jakarta.persistence.*
import paladin.router.enums.configuration.Broker
import java.util.*

@JsonIgnoreProperties(ignoreUnknown = true)
@Entity
@Table(
    name = "dispatch_topic", schema = "event_routing",
    indexes = [
        Index(name = "idx_dispatch_producer_id", columnList = "producer_id"),
    ],
    uniqueConstraints = [
        UniqueConstraint(columnNames = ["producer_id", "source_topic", "destination_topic"])
    ]
)
data class DispatchTopicConfigurationEntity(
    @Id
    @GeneratedValue
    @Column(name = "id", columnDefinition = "UUID DEFAULT uuid_generate_v4()", nullable = false)
    val id: UUID? = null,

    @Column(name = "producer_id", nullable = false, columnDefinition = "UUID")
    val producerId: UUID,

    @Column(name = "source_topic", nullable = false, length = 255, columnDefinition = "VARCHAR(255)")
    var sourceTopic: String,

    @Column(name = "destination_topic", nullable = false, length = 255, columnDefinition = "VARCHAR(255)")
    var destinationTopic: String,

    @Column(name = "key_format", nullable = true, length = 255, columnDefinition = "VARCHAR(255)")
    @Enumerated(EnumType.STRING)
    val keyFormat: Broker.ProducerFormat? = null,

    @Column(name = "key_schema", nullable = true, columnDefinition = "TEXT")
    val keySchema: String? = null,

    @Column(name = "value_format", nullable = false, length = 255, columnDefinition = "VARCHAR(255)")
    @Enumerated(EnumType.STRING)
    val valueFormat: Broker.ProducerFormat,

    @Column(name = "value_schema", nullable = true, columnDefinition = "TEXT")
    val valueSchema: String? = null
)