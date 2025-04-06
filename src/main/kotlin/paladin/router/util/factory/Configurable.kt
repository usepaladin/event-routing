package paladin.router.util.factory

interface Configurable {
    fun updateConfiguration(config: Configurable): Configurable
}