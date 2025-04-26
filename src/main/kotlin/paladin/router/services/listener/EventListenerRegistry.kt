package paladin.router.services.listener

import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.stereotype.Service
import paladin.router.pojo.listener.EventListener
import java.util.concurrent.ConcurrentHashMap

@Service
class EventListenerRegistry(

) {

    private val listeners = ConcurrentHashMap<String, EventListener<*,*>>()
}