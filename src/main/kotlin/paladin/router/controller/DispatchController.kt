package paladin.router.controller

import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import paladin.router.pojo.dispatch.MessageDispatcher
import paladin.router.services.dispatch.DispatchService

@RestController
@RequestMapping("/api/dispatch")
class DispatchController(
    private val dispatchService: DispatchService
) {

    @GetMapping("/")
    fun getAllDispatchers(): ResponseEntity<List<MessageDispatcher>>{
        val dispatchers: List<MessageDispatcher> = dispatchService.getAllDispatchers()
        return ResponseEntity.ok(dispatchers)
    }

    @GetMapping("/{brokerName}")
    fun getDispatcher(@PathVariable brokerName: String): ResponseEntity<MessageDispatcher>{
        val dispatcher: MessageDispatcher? = dispatchService.getDispatcher(brokerName)

        return if (dispatcher != null) {
            ResponseEntity.ok(dispatcher)
        } else {
            ResponseEntity.notFound().build()
        }
    }
}