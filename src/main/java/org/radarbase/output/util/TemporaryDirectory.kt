/*
 * Copyright 2018 The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarbase.output.util

import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.IOException
import java.nio.file.Path
import kotlin.io.path.createDirectories
import kotlin.io.path.createTempDirectory
import kotlin.io.path.exists

/** Temporary directory that will be removed on close or shutdown.  */
class TemporaryDirectory(root: Path, prefix: String) : Closeable {

    private val shutdownHook: Thread
    val path: Path

    init {
        root.createDirectories()
        path = createTempDirectory(root, prefix)
        shutdownHook = Thread({ this.doClose() },
            "remove-" + path.toString().replace("/".toRegex(), "-"))
        try {
            Runtime.getRuntime().addShutdownHook(shutdownHook)
        } catch (ex: IllegalStateException) {
            close()
            throw ex
        }

    }

    private fun doClose() {
        try {
            // Ignore errors the first time
            deleteTree()
        } catch (ex: IOException) {
            // ignore the first time
        }

        try {
            if (path.exists()) {
                try {
                    Thread.sleep(500L)
                } catch (ex: InterruptedException) {
                    logger.debug("Waiting for temporary directory deletion interrupted")
                    Thread.currentThread().interrupt()
                }

                deleteTree()
            }
        } catch (ex: IOException) {
            logger.warn("Cannot delete temporary directory {}: {}", path, ex.toString())
        }
    }

    private fun deleteTree() {
        path.toFile().walkBottomUp()
            .forEach {
                try {
                    it.delete()
                } catch (ex: Exception) {
                    logger.warn("Cannot delete temporary path {}: {}",
                        it, ex.toString())
                }
            }
    }

    override fun close() {
        doClose()
        try {
            Runtime.getRuntime().removeShutdownHook(shutdownHook)
        } catch (ex: IllegalStateException) {
            // already shutting down
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(TemporaryDirectory::class.java)
    }
}
