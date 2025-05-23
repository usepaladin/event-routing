package paladin.router.controller

import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import paladin.router.dto.MessageDispatchDTO
import paladin.router.models.configuration.producers.ProducerCreationRequest
import paladin.router.models.dispatch.MessageDispatcher
import paladin.router.services.producers.ProducerService

@RestController
@RequestMapping("/api/producer")
class ProducerConfigurationController(
    private val producerService: ProducerService
) {
    @PostMapping("/")
    fun registerProducer(@RequestBody request: ProducerCreationRequest): ResponseEntity<MessageDispatchDTO> {
        val createdProducer: MessageDispatcher = producerService.registerProducer(request)
        return ResponseEntity.status(HttpStatus.CREATED).body(MessageDispatchDTO.fromEntity(createdProducer))
    }

    @DeleteMapping("/{name}")
    fun deleteProducer(@PathVariable name: String): ResponseEntity<Unit> {
        producerService.deleteProducer(name)
        return ResponseEntity.status(HttpStatus.NO_CONTENT).build()
    }
}