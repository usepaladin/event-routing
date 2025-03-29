package paladin.router.controller

import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/broker")
class BrokerConfigurationController {

    fun createBroker(){}
    fun deleteBroker(){}
    fun updateBroker(){}
    fun getBrokers(){}
    fun getBroker(@PathVariable brokerName: String){}
}