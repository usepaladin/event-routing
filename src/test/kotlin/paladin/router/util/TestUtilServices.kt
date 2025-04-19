package paladin.router.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule

object TestUtilServices{
    public val objectMapper = ObjectMapper().registerKotlinModule()
}