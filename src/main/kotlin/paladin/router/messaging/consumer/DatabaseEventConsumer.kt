package paladin.router.messaging.consumer

import io.github.oshai.kotlinlogging.KLogger
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Service
import paladin.avro.database.ChangeEventData
import paladin.router.configuration.properties.CoreConfigurationProperties
import paladin.avro.database.DatabaseEventRouterValueAv
import paladin.router.services.dispatch.DispatchService

@Service
class DatabaseEventConsumer(
    private val dispatchService: DispatchService,
    private val logger: KLogger,
    private val config: CoreConfigurationProperties
) {

    /**
     * Route events generated from the Database Event discovery service
     * to the appropriate message broker configured by the user
     */
    @KafkaListener(
        topics = ["event-routing-database"],
        groupId = "event-router-tenant-\${TENANT_ID}",
        autoStartup = "false"
    )
    fun routeDatabaseChangeEvent(
        @Payload event: DatabaseEventRouterValueAv,
        @Header(KafkaHeaders.RECEIVED_KEY, required = true) key: String
    ) {
        val dispatchEvent: List<DispatchEvent<ChangeEventData>> = DispatchEvent.fromEvent(event)
        dispatchService.dispatchEvents(dispatchEvent)
    }


}