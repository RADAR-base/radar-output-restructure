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

package org.radarbase.hdfs.data

import java.io.BufferedReader
import java.io.IOException
import java.io.InputStream
import java.io.InputStreamReader
import java.nio.file.Path
import org.radarbase.hdfs.Plugin

interface StorageDriver : Plugin {
    /** Query the path status. Returns null if the file does not exist. */
    fun status(path: Path): PathStatus?

    @Throws(IOException::class)
    fun newInputStream(path: Path): InputStream

    @Throws(IOException::class)
    fun move(oldPath: Path, newPath: Path)

    @Throws(IOException::class)
    fun store(localPath: Path, newPath: Path)

    @Throws(IOException::class)
    fun delete(path: Path)

    @Throws(IOException::class)
    fun newBufferedReader(path: Path): BufferedReader {
        val reader = InputStreamReader(newInputStream(path))
        return BufferedReader(reader)
    }

    data class PathStatus(val size: Long)
}
