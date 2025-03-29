package paladin.router.repository

import org.springframework.data.jpa.repository.JpaRepository
import paladin.router.entities.brokers.configuration.MessageBrokerConfigurationEntity
import java.util.UUID

interface MessageBrokerRepository: JpaRepository<MessageBrokerConfigurationEntity, UUID> {
}