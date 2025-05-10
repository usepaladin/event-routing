package util.mock

data class Operation(val id: String, val operation: OperationType) {
    enum class OperationType {
        CREATE,
        UPDATE,
        DELETE
    }

    companion object {
        val SCHEMA = """
            {
                "\${'$'}schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {
                    "id": {
                        "type": "string"
                    },
                    "operation": {
                        "type": "string",
                        "enum": ["CREATE", "UPDATE", "DELETE"]
                    }
                },
                "required": ["id", "operation"]
            }
        """.trimIndent()
    }
}

data class User(val name: String, val age: Int, val email: String? = null) {
    companion object {
        val SCHEMA = """
            {
                "\${'$'}schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string"
                    },
                    "age": {
                        "type": "integer"
                    },
                    "email": {
                        "type": "string",
                        "format": "email"
                    }
                },
                "required": ["name", "age"]
            }
        """.trimIndent()
    }
}

