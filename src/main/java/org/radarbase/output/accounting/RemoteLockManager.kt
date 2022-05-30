package org.radarbase.output.accounting

import org.radarbase.output.util.SuspendedCloseable
import org.radarbase.output.util.SuspendedCloseable.Companion.useSuspended

interface RemoteLockManager {
    suspend fun acquireLock(name: String): RemoteLock?
    suspend fun <T> tryWithLock(
        name: String,
        action: suspend () -> T,
    ): T? = acquireLock(name)?.useSuspended {
        action()
    }

    interface RemoteLock : SuspendedCloseable
}
