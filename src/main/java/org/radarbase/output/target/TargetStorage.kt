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

import java.io.BufferedReader
import java.io.IOException
import java.io.InputStream
import java.nio.file.Path

interface TargetStorage {
    suspend fun initialize()

    /**
     * Query the path status.
     * @return path status or null if the file does not exist.
     */
    suspend fun status(path: Path): PathStatus?

    /**
     * Read a file from storage.
     * It should be closed by the caller.
     */
    @Throws(IOException::class)
    suspend fun newInputStream(path: Path): InputStream

    /** Move a file on the target storage to a new location. */
    @Throws(IOException::class)
    suspend fun move(oldPath: Path, newPath: Path)

    /** Store a local file to the target storage. */
    @Throws(IOException::class)
    suspend fun store(localPath: Path, newPath: Path)

    /** Delete a file on the target storage. Will not delete directories. */
    @Throws(IOException::class)
    suspend fun delete(path: Path)

    /**
     * Read a file from storage using a buffered reader.
     * It should be closed by the caller.
     */
    @Throws(IOException::class)
    suspend fun newBufferedReader(path: Path): BufferedReader =
        newInputStream(path).bufferedReader()

    /** Create given directory, by recursively creating all parent directories. */
    @Throws(IOException::class)
    suspend fun createDirectories(directory: Path?)

    data class PathStatus(
        /** Size in bytes */
        val size: Long,
    )

    fun allowsPrefix(prefix: String): Boolean {
        return true
    }
}
