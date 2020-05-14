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

import org.radarbase.output.config.LocalConfig
import org.slf4j.LoggerFactory
import java.io.IOException
import java.io.InputStream
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption.ATOMIC_MOVE
import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import java.nio.file.attribute.PosixFilePermissions

class LocalTargetStorage(private val config: LocalConfig) : TargetStorage {
    init {
        logger.info("Local storage configured with user id {}:{} (-1 if not configured)",
                config.userId, config.groupId)
    }

    @Throws(IOException::class)
    override fun status(path: Path): TargetStorage.PathStatus? {
        return if (Files.exists(path)) {
            TargetStorage.PathStatus(Files.size(path))
        } else {
            null
        }
    }

    @Throws(IOException::class)
    override fun newInputStream(path: Path): InputStream = Files.newInputStream(path)

    @Throws(IOException::class)
    override fun move(oldPath: Path, newPath: Path) {
        try {
            Files.move(oldPath, newPath, REPLACE_EXISTING, ATOMIC_MOVE)
        } catch (ex: AtomicMoveNotSupportedException) {
            Files.move(oldPath, newPath, REPLACE_EXISTING)
        }
    }

    @Throws(IOException::class)
    override fun store(localPath: Path, newPath: Path) {
        localPath.updateUser()
        Files.setPosixFilePermissions(localPath, PosixFilePermissions.fromString("rw-r--r--"))
        move(localPath, newPath)
    }

    override fun createDirectories(directory: Path) {
        Files.createDirectories(directory, PosixFilePermissions.asFileAttribute(
                PosixFilePermissions.fromString("rwxr-xr-x")))

        directory.updateUser()
    }

    private fun Path.updateUser() {
        if (config.userId >= 0) {
            Files.setAttribute(this, "unix:uid", config.userId)
        }
        if (config.groupId >= 0) {
            Files.setAttribute(this, "unix:gid", config.groupId)
        }
    }

    @Throws(IOException::class)
    override fun delete(path: Path) = Files.delete(path)

    companion object {
        private val logger = LoggerFactory.getLogger(LocalTargetStorage::class.java)
    }
}
