package paladin.router.controller

import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import paladin.router.models.dispatch.DispatchTopic
import paladin.router.models.dispatch.DispatchTopicRequest
import paladin.router.services.dispatch.DispatchService

@RestController
@RequestMapping("/api/topic")
class DispatchTopicController(private val dispatchService: DispatchService) {

    @GetMapping("/")
    fun getAllTopics(): ResponseEntity<List<DispatchTopic>> {
        val topics: List<DispatchTopic> = dispatchService.getAllTopics()
        return ResponseEntity.ok(topics)
    }

    @GetMapping("/{topic}")
    fun getDispatchersForTopic(@PathVariable topic: String): ResponseEntity<List<DispatchTopic>> {
        val dispatchers: List<DispatchTopic> = dispatchService.getDispatchersOnTopic(topic)
        return ResponseEntity.ok(dispatchers)
    }

    @GetMapping("/dispatcher/{dispatcher}")
    fun getAllTopicsForDispatcher(@PathVariable dispatcher: String): ResponseEntity<List<DispatchTopic>> {
        val topics: List<DispatchTopic> = dispatchService.getTopicsForDispatcher(dispatcher)
        return ResponseEntity.ok(topics)
    }

    @GetMapping("/{topic}/{dispatcher}")
    fun getDispatchTopicForDispatcher(
        @PathVariable topic: String,
        @PathVariable dispatcher: String
    ): ResponseEntity<DispatchTopic> {
        TODO()
    }

    @PostMapping("/")
    fun addDispatcherTopic(@RequestBody dispatcherTopic: DispatchTopicRequest): ResponseEntity<DispatchTopic> {
        val createdTopic: DispatchTopic = dispatchService.addDispatcherTopic(dispatcherTopic)
        return ResponseEntity.status(HttpStatus.CREATED).body(createdTopic)
    }

    @PutMapping("/")
    fun updateDispatcherTopic(
        @RequestBody dispatcherTopic: DispatchTopicRequest
    ): ResponseEntity<DispatchTopic> {
        val updatedTopic: DispatchTopic = dispatchService.editDispatcherForTopic(dispatcherTopic)
        return ResponseEntity.ok(updatedTopic)
    }

    @DeleteMapping("/{topic}/{dispatcher}")
    fun removeDispatcherFromTopic(
        @PathVariable topic: String,
        @PathVariable dispatcher: String
    ): ResponseEntity<Void> {
        dispatchService.removeDispatcherFromTopic(topic, dispatcher)
        return ResponseEntity.noContent().build()
    }

}