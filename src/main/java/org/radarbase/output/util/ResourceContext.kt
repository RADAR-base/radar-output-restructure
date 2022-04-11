package org.radarbase.output.util

import java.io.Closeable
import java.io.IOException

/**
 * Resource context to use multiple resources in.
 * Replaces Java try-with-resources with multiple resources. Use by calling
 * ```
 * resourceContext {
 *      val reader = resourceChain { path.reader() }
 *          .chain { it.buffered() }
 *          .conclude { CSVReader(it) }
 *      val writer = createResource { outPath.writer() }
 *
 *      reader.lines().forEach { writer.write(it) }
 * }
 * ```
 * Instances of this class may only be used from a single thread. When the resourceContext is
 * finished, the managed resources are closed in the reversed order that they were created.
 */
class ResourceContext: Closeable {
    private var isClosed = false
    private val resources: MutableList<AutoCloseable> = mutableListOf()

    /** Add a given resource to be closed when this ResourceContext is closed. */
    fun resource(resource: AutoCloseable) {
        resources += resource
    }

    /** Create a resource with [supplier] to be closed when this ResourceContext is closed. */
    inline fun <T: AutoCloseable> createResource(supplier: () -> T): T = supplier()
        .also { resource(it) }

    /**
     * Create a resource chain with [supplier] to be closed when this ResourceContext is closed.
     * The chain can be extended with more [Chain.chain] calls and finished with [Chain.conclude].
     */
    inline fun <T: AutoCloseable> resourceChain(supplier: () -> T): Chain<T> {
        return Chain(createResource(supplier))
    }

    inner class Chain<T: AutoCloseable>(
        val result: T,
    ) {
        /**
         * Chain next resource from [supplier] with the previous [result] as an argument.
         * @return another resource chain to do further chaining with.
         */
        inline fun <R: AutoCloseable> chain(
            supplier: (T) -> R,
        ): Chain<R> = Chain(conclude(supplier))

        /**
         * Create the final resource in the chain using [supplier].
         * It takes the previous [result] as an argument.
         * @return resource to use directly.
         */
        inline fun <R: AutoCloseable> conclude(
            supplier: (T) -> R,
        ): R = supplier(result)
            .also { resource(it) }
    }

    /**
     * Close this resource and all resources managed by it.
     * Resources are closed in the reverse order in which they were created. The first (innermost)
     * exception that is thrown is re-thrown. Further exceptions are added as kotlin suppressed
     * exceptions.
     */
    override fun close() {
        if (isClosed) {
            throw IOException("ResourceContext is already closed")
        }
        isClosed = true
        var throwable: Throwable? = null
        resources.reversed()
            .forEach {
                try {
                    it.close()
                } catch (ex: Throwable) {
                    val localThrowable = throwable
                    if (localThrowable == null) {
                        throwable = ex
                    } else {
                        localThrowable.addSuppressed(ex)
                    }
                }
            }
        throwable?.let { throw it }
    }

    companion object {
        inline fun <T> resourceContext(exec: ResourceContext.() -> T): T = ResourceContext().use(exec)
    }
}
