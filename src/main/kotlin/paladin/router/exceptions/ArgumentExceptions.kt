package paladin.router.exceptions

class InvalidArgumentException(message: String): RuntimeException(message)
class UnauthorizedException(message: String): RuntimeException(message)