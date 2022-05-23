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

package org.radarbase.output.target

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.radarbase.output.config.LocalConfig
import org.slf4j.LoggerFactory
import java.io.IOException
import java.io.InputStream
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Path
import java.nio.file.StandardCopyOption.ATOMIC_MOVE
import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import java.nio.file.attribute.PosixFilePermissions
import kotlin.io.path.*

class LocalTargetStorage(private val config: LocalConfig) : TargetStorage {
    init {
        logger.info(
            "Local storage configured with user id {}:{} (-1 if not configured)",
            config.userId,
            config.groupId,
        )
    }

    override suspend fun initialize() = Unit

    @Throws(IOException::class)
    override suspend fun status(path: Path): TargetStorage.PathStatus? =
        withContext(Dispatchers.IO) {
            if (path.exists()) {
                TargetStorage.PathStatus(path.fileSize())
            } else {
                null
            }
        }

    @Throws(IOException::class)
    override suspend fun newInputStream(path: Path): InputStream = withContext(Dispatchers.IO) {
        path.inputStream()
    }

    @Throws(IOException::class)
    override suspend fun move(oldPath: Path, newPath: Path) = withContext(Dispatchers.IO) {
        doMove(oldPath, newPath)
    }

    private fun doMove(oldPath: Path, newPath: Path) {
        try {
            oldPath.moveTo(newPath, REPLACE_EXISTING, ATOMIC_MOVE)
        } catch (ex: AtomicMoveNotSupportedException) {
            oldPath.moveTo(newPath, REPLACE_EXISTING)
        }
    }

    @Throws(IOException::class)
    override suspend fun store(localPath: Path, newPath: Path) = withContext(Dispatchers.IO) {
        localPath.updateUser()
        localPath.setPosixFilePermissions(PosixFilePermissions.fromString("rw-r--r--"))
        doMove(localPath, newPath)
    }

    override fun createDirectories(directory: Path) {
        directory.createDirectories(
            PosixFilePermissions.asFileAttribute(
                PosixFilePermissions.fromString("rwxr-xr-x"),
            ),
        )
        directory.updateUser()
    }

    private fun Path.updateUser() {
        if (config.userId >= 0) {
            setAttribute("unix:uid", config.userId)
        }
        if (config.groupId >= 0) {
            setAttribute("unix:gid", config.groupId)
        }
    }

    @Throws(IOException::class)
    override suspend fun delete(path: Path) = path.deleteExisting()

    companion object {
        private val logger = LoggerFactory.getLogger(LocalTargetStorage::class.java)
    }
}
