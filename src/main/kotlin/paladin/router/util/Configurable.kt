package paladin.router.util

/**
+ * Interface for objects that can have their configuration updated at runtime.
+ * Implemented by configuration classes that need to support dynamic updates.
+ */
interface Configurable {

     /** Updates this configuration with values from another configuration object.
     *
     * @param config The configuration containing new values
     * @return This configurable instance after updating, to support method chaining
     * @throws IllegalArgumentException if the provided config is not compatible
     */
    fun updateConfiguration(config: Configurable): Configurable
}