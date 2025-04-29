package paladin.router.repository

import org.springframework.data.jpa.repository.JpaRepository
import paladin.router.entities.listener.EventListenerConfigurationEntity
import java.util.*

interface EventListenerRepository : JpaRepository<EventListenerConfigurationEntity, UUID>