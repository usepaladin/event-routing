package paladin.router.controller

import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.PutMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import paladin.router.models.dispatch.DispatchTopic
import paladin.router.models.dispatch.DispatchTopicRequest
import paladin.router.services.dispatch.DispatchService

@RestController
@RequestMapping("/api/topic")
class DispatchTopicController(dispatchService: DispatchService) {

    @GetMapping("/")
    fun getAllTopics(): ResponseEntity<List<DispatchTopic>> {
        TODO()
    }

    @GetMapping("/{topic}")
    fun getDispatchersForTopic(@PathVariable topic: String): List<DispatchTopic>{
        TODO()
    }

    @GetMapping("/{topic}/{dispatcher}")
    fun getDispatchTopicForDispatcher(
        @PathVariable topic: String,
        @PathVariable dispatcher: String
    ): ResponseEntity<DispatchTopic> {
        TODO()
    }

    @PostMapping("/")
    fun addDispatcherTopic(@RequestBody dispatcherTopic: DispatchTopicRequest){
        TODO()
    }

    @PutMapping("/{topic}")
    fun updateDispatcherTopic(
        @PathVariable topic: String,
        @RequestBody dispatcherTopic: DispatchTopicRequest
    ): ResponseEntity<DispatchTopic> {
        TODO()
    }

}