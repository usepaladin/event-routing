package paladin.router.services.dispatch

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
@SpringBootTest
class DispatchIntegrationTest {

    companion object {

        @DynamicPropertySource
        @JvmStatic
        fun dynamicProperties(registry: org.springframework.test.context.DynamicPropertyRegistry) {
        }

        @BeforeAll
        @JvmStatic
        fun setup() {
            // Setup code here
        }

        @AfterAll
        @JvmStatic
        fun tearDown() {
            // Teardown code here
        }
    }
}