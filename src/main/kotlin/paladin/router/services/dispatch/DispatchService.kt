package paladin.router.services.dispatch

import org.apache.avro.specific.SpecificRecord
import org.springframework.stereotype.Service
import paladin.router.pojo.dispatch.DispatchEvent

@Service
class DispatchService {

    fun <T: SpecificRecord> dispatchEvent(event: DispatchEvent<T>){

    }
}