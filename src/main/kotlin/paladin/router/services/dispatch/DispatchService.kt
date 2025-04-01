package paladin.router.services.dispatch

import io.github.oshai.kotlinlogging.KLogger
import org.apache.avro.specific.SpecificRecord
import org.springframework.stereotype.Service
import paladin.router.pojo.dispatch.DispatchEvent
import paladin.router.pojo.dispatch.MessageDispatcher
import java.util.concurrent.ConcurrentHashMap

@Service
class DispatchService(private val logger: KLogger) {
    private val clientDispatchers = ConcurrentHashMap<String, MessageDispatcher>()

    fun <T: SpecificRecord> dispatchEvent(event: DispatchEvent<T>){

    }


}