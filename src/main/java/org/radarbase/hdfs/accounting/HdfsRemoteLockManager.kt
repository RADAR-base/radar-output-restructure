package org.radarbase.hdfs.accounting

import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.lang.Exception

class HdfsRemoteLockManager(
        private val fileSystem: FileSystem,
        private val lockPath: Path
): RemoteLockManager {
    override fun acquireTopicLock(topic: String): RemoteLockManager.RemoteLock? {
        val topicLockPath = Path(lockPath, "$topic.lock")
        return try {
            HdfsRemoteLock(topicLockPath, fileSystem.create(topicLockPath, false, 0, 1, 1))
        } catch (ex: Exception) {
            null
        }
    }

    private inner class HdfsRemoteLock(
            private val topicLockPath: Path,
            private val output: FSDataOutputStream
    ): RemoteLockManager.RemoteLock {
        override fun close() {
            output.close()
            fileSystem.delete(topicLockPath, false)
        }
    }
}
