package paladin.router.repository

import org.springframework.data.jpa.repository.JpaRepository
import paladin.router.entities.brokers.configuration.MessageProducerConfigurationEntity
import java.util.*

interface MessageProducerRepository : JpaRepository<MessageProducerConfigurationEntity, UUID>