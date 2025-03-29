package paladin.router.services.brokers

import io.github.oshai.kotlinlogging.KLogger
import org.springframework.stereotype.Service
import paladin.router.models.configuration.brokers.MessageBroker
import paladin.router.repository.MessageBrokerRepository
import java.util.concurrent.ConcurrentHashMap

@Service
class BrokerService(private val messageBrokerRepository: MessageBrokerRepository, private val kLogger: KLogger) {
    private val messageBrokers = ConcurrentHashMap<String, MessageBroker>()

}