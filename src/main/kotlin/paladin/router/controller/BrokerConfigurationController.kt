package paladin.router.controller

import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import paladin.router.models.configuration.brokers.MessageBroker
import paladin.router.pojo.configuration.brokers.BrokerCreationRequest
import paladin.router.pojo.dispatch.MessageDispatcher
import paladin.router.services.brokers.BrokerService

@RestController
@RequestMapping("/api/broker")
class BrokerConfigurationController(
    private val brokerService: BrokerService
) {
    @PostMapping("/")
    fun createBroker(@RequestBody broker: BrokerCreationRequest): ResponseEntity<MessageDispatcher>{
        val createdBroker: MessageDispatcher = brokerService.createBroker(broker)
        return ResponseEntity.status(HttpStatus.CREATED).body(createdBroker)
    }

    @DeleteMapping("/{brokerName}")
    fun deleteBroker(@PathVariable brokerName: String): ResponseEntity<Unit>{
        brokerService.deleteBroker(brokerName)
        return ResponseEntity.status(HttpStatus.NO_CONTENT).build()
    }

    @PutMapping("/")
    fun updateBroker(@RequestBody dispatcher: MessageDispatcher): ResponseEntity<MessageDispatcher>{
        val updatedBroker: MessageDispatcher = brokerService.updateBroker(dispatcher)
        return ResponseEntity.status(HttpStatus.OK).body(updatedBroker)
    }
}