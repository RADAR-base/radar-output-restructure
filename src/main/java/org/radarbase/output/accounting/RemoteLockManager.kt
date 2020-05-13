package org.radarbase.output.accounting

import java.io.Closeable

interface RemoteLockManager {
    fun acquireTopicLock(topic: String): RemoteLock?

    interface RemoteLock: Closeable
}
