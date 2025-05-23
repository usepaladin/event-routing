package paladin.router.exceptions.handler

import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ControllerAdvice
import org.springframework.web.bind.annotation.ExceptionHandler
import paladin.router.exceptions.*
import paladin.router.models.exceptions.ErrorResponse
import java.io.PrintWriter
import java.io.StringWriter

@ControllerAdvice
class GlobalExceptionHandler {
    fun appendStackTraceToCustomError(ex: Throwable, err: ErrorResponse): Unit {
        val stringWriter = StringWriter()
        ex.printStackTrace(PrintWriter(stringWriter))
        err.stackTrace = stringWriter.toString()
    }

    fun handleException(
        ex: Throwable,
        status: HttpStatus,
        includeStackTrace: Boolean = false
    ): ResponseEntity<ErrorResponse> {
        val errorMessage = ex.message ?: "Unknown error occurred"
        val errorResponse = ErrorResponse(status, errorMessage)

        if (includeStackTrace) {
            appendStackTraceToCustomError(ex, errorResponse)
        }
        return ResponseEntity.status(status).body(errorResponse)
    }

    @ExceptionHandler(InvalidArgumentException::class)
    fun handleInvalidArgumentException(ex: InvalidArgumentException): ResponseEntity<ErrorResponse> {
        return handleException(ex, HttpStatus.BAD_REQUEST, true)
    }

    @ExceptionHandler(UnauthorizedException::class)
    fun handleUnauthorizedException(ex: UnauthorizedException): ResponseEntity<ErrorResponse> {
        return handleException(ex, HttpStatus.UNAUTHORIZED, true)
    }

    @ExceptionHandler(ProducerNotFoundException::class)
    fun handleProducerNotFoundException(ex: ProducerNotFoundException): ResponseEntity<ErrorResponse> {
        return handleException(ex, HttpStatus.NOT_FOUND, true)
    }

    @ExceptionHandler(ActiveListenerException::class)
    fun handleActiveListenerException(ex: ActiveListenerException): ResponseEntity<ErrorResponse> {
        return handleException(ex, HttpStatus.FORBIDDEN, true)
    }

    @ExceptionHandler(ListenerNotFoundException::class)
    fun handleListenerNotFoundException(ex: ListenerNotFoundException): ResponseEntity<ErrorResponse> {
        return handleException(ex, HttpStatus.NOT_FOUND, true)
    }

}