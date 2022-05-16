package org.radarbase.output.util

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

class SuspendedCloseableWrapper<T: AutoCloseable>(
    val wrapped: T,
): SuspendedCloseable {
    override suspend fun closeAndJoin() = withContext(Dispatchers.IO) {
        wrapped.close()
    }
}
