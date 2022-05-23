package org.radarbase.output.util

interface SuspendedCloseable {
    suspend fun closeAndJoin()

    companion object {
        suspend inline fun <S : SuspendedCloseable?, T> S.useSuspended(action: (S) -> T): T {
            return if (this == null) {
                @Suppress("KotlinConstantConditions")
                return action(this)
            } else {
                var exception: Throwable? = null
                try {
                    action(this)
                } catch (e: Throwable) {
                    exception = e
                    throw e
                } finally {
                    if (exception == null) {
                        closeAndJoin()
                    } else {
                        try {
                            closeAndJoin()
                        } catch (closeException: Throwable) {
                            exception.addSuppressed(closeException)
                        }
                    }
                }
            }
        }

        suspend inline fun <S : AutoCloseable?, T> S.useSuspended(action: (S) -> T): T {
            return if (this == null) {
                @Suppress("KotlinConstantConditions")
                action(this)
            } else {
                SuspendedCloseableWrapper(this).useSuspended { v ->
                    action(v.wrapped)
                }
            }
        }
    }
}
