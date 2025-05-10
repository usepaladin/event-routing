package paladin.router.controller

import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import paladin.router.dto.MessageDispatchDTO
import paladin.router.models.dispatch.MessageDispatcher
import paladin.router.services.dispatch.DispatchService

@RestController
@RequestMapping("/api/dispatch")
class DispatchController(
    private val dispatchService: DispatchService
) {

    @GetMapping("/")
    fun getAllDispatchers(): ResponseEntity<List<MessageDispatchDTO>> {
        val dispatchers: List<MessageDispatcher> = dispatchService.getAllDispatchers()
        return ResponseEntity.ok(dispatchers.map { MessageDispatchDTO.fromEntity(it) })
    }

    @GetMapping("/{brokerName}")
    fun getDispatcher(@PathVariable brokerName: String): ResponseEntity<MessageDispatchDTO> {
        val dispatcher: MessageDispatcher =
            dispatchService.getDispatcher(brokerName) ?: return ResponseEntity.notFound().build()
        return ResponseEntity.ok(MessageDispatchDTO.fromEntity(dispatcher))
    }
}