package org.radarbase.output.accounting

import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory
import java.lang.Exception

class HdfsRemoteLockManager(
        private val fileSystem: FileSystem,
        private val lockPath: Path
): RemoteLockManager {
    init {
        fileSystem.mkdirs(lockPath)
    }

    override fun acquireTopicLock(topic: String): RemoteLockManager.RemoteLock? {
        val topicLockPath = Path(lockPath, "$topic.lock")
        return try {
            HdfsRemoteLock(topicLockPath, fileSystem.create(topicLockPath, true, 1, 1, MINIMUM_BLOCK_SIZE))
        } catch (ex: FileAlreadyExistsException) {
            logger.info("Topic {} is locked by another process. Skipping.", topicLockPath)
            null
        } catch (ex: Exception) {
            logger.warn("Failed to acquire HDFS lease of {}: {}", topicLockPath, ex.toString())
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

    companion object {
        private val logger = LoggerFactory.getLogger(HdfsRemoteLockManager::class.java)
        private const val MINIMUM_BLOCK_SIZE: Long = 1024 * 1024
    }
}
