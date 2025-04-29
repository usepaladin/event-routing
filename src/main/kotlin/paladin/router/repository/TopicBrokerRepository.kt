package paladin.router.repository

import org.springframework.data.jpa.repository.JpaRepository
import paladin.router.entities.dispatch.DispatchTopicConfigurationEntity
import java.util.UUID

interface DispatchTopicRepository : JpaRepository<DispatchTopicConfigurationEntity, UUID> {
}