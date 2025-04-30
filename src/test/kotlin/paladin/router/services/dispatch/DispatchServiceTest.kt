package paladin.router.services.dispatch

import io.github.oshai.kotlinlogging.KLogger
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import kotlinx.coroutines.ExperimentalCoroutinesApi
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.extension.ExtendWith

@ExperimentalCoroutinesApi
@ExtendWith(MockKExtension::class)
class DispatchServiceTest {

    @MockK
    private lateinit var logger: KLogger

    @MockK
    private lateinit var topicService: DispatchTopicService

    private lateinit var dispatchService: DispatchService

    @BeforeEach
    fun setUp() {
        dispatchService = DispatchService(topicService, logger)
    }

}