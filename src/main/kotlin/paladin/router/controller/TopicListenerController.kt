package paladin.router.controller

import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import paladin.router.dto.EventListenerDTO
import paladin.router.models.listener.ListenerRegistrationRequest
import paladin.router.services.listener.EventListenerRegistry

@RestController
@RequestMapping("/api/listener")
class TopicListenerController(private val eventListenerRegistry: EventListenerRegistry) {

    @GetMapping("/")
    fun getAllListeners(): ResponseEntity<List<EventListenerDTO>> {
        eventListenerRegistry.getAllTopicListeners().let { listener ->
            return ResponseEntity.ok(listener.map { EventListenerDTO.from(it) })
        }
    }

    @GetMapping("/{topic}")
    fun getListenerForTopic(@PathVariable topic: String): ResponseEntity<EventListenerDTO> {
        eventListenerRegistry.getListener(topic).let {
            if (it == null) {
                return ResponseEntity.notFound().build()
            }

            val eventListener = EventListenerDTO.from(it)

            return ResponseEntity.ok(eventListener)
        }
    }

    @PostMapping("/")
    fun addListener(@RequestBody listener: ListenerRegistrationRequest): ResponseEntity<EventListenerDTO> {
        eventListenerRegistry.registerListener(listener).let {
            val createdListener = EventListenerDTO.from(it)
            return ResponseEntity.status(HttpStatus.CREATED).body(createdListener)
        }
    }

    @PutMapping("/")
    fun updateListener(@RequestBody listener: ListenerRegistrationRequest): ResponseEntity<EventListenerDTO> {
        eventListenerRegistry.editListener(listener).let {
            val updatedListener = EventListenerDTO.from(it)
            return ResponseEntity.status(HttpStatus.OK).body(updatedListener)
        }
    }

    @DeleteMapping("/{topic}")
    fun deleteListener(@PathVariable topic: String): ResponseEntity<Unit> {
        eventListenerRegistry.unregisterListener(topic)
        return ResponseEntity.status(HttpStatus.NO_CONTENT).build()
    }

    @PostMapping("/{topic}/start")
    fun startListener(@PathVariable topic: String): ResponseEntity<Unit> {
        eventListenerRegistry.startListener(topic)
        return ResponseEntity.status(HttpStatus.OK).build()
    }

    @PostMapping("/{topic}/pause")
    fun pauseListener(@PathVariable topic: String): ResponseEntity<Unit> {
        eventListenerRegistry.stopListener(topic)
        return ResponseEntity.status(HttpStatus.OK).build()
    }
}