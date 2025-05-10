package paladin.router.models.exceptions

import org.springframework.http.HttpStatus

data class ErrorResponse(val statusCode: HttpStatus, val message: String, var stackTrace: String? = null)
