package paladin.router.services.brokers

import io.github.oshai.kotlinlogging.KLogger
import org.springframework.stereotype.Service
import paladin.router.repository.MessageBrokerRepository
import paladin.router.services.dispatch.DispatchService


@Service
class BrokerService(
    private val messageBrokerRepository: MessageBrokerRepository,
    private val kLogger: KLogger,
    private val dispatchService: DispatchService,
) {

    fun addBroker(){

    }
    fun updateBroker(){

    }
    fun removeBroker(){
        dispatchService.removeDispatcher("brokerName")
    }

}