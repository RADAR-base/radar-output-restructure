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

import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.io.IOException
import java.util.concurrent.TimeUnit

/**
 * File writer where data is written in a separate thread with a timeout.
 *
 * @param name thread name.
 * @param timeout maximum time between triggering a write and actually writing the file.
 * @param timeoutUnit timeout unit.
 */
abstract class PostponedWriter(
    private val coroutineScope: CoroutineScope,
    private val name: String,
    timeout: Long,
    timeoutUnit: TimeUnit,
) : SuspendedCloseable {
    private var writeFuture: Job? = null
    private val modificationMutex = Mutex()
    private val workMutex = Mutex()
    private var isClosed = false
    private val timeout = timeoutUnit.toMillis(timeout)

    /**
     * Trigger a write to occur within set timeout. If a write was already triggered earlier but has
     * not yet taken place, the write will occur earlier.
     */
    suspend fun triggerWrite() {
        modificationMutex.withLock {
            if (isClosed || writeFuture != null) {
                return
            }

            writeFuture = coroutineScope.launch {
                delay(timeout)
                modificationMutex.withLock {
                    if (isClosed) {
                        return@launch
                    }
                    writeFuture = null
                }
                workMutex.withLock {
                    doWrite()
                }
            }
        }
    }

    /** Perform the write.  */
    protected abstract suspend fun doWrite()

    override suspend fun closeAndJoin() {
        modificationMutex.withLock {
            isClosed = true
        }
        doFlush(true)
    }

    @Throws(IOException::class)
    private suspend fun doFlush(shutdown: Boolean) {
        coroutineScope {
            launch {
                modificationMutex.withLock {
                    if (!shutdown && isClosed) return@launch

                    writeFuture?.cancel()
                    writeFuture = null
                }
                workMutex.withLock {
                    doWrite()
                }
            }
        }
    }

    @Throws(IOException::class)
    suspend fun flush() {
        doFlush(false)
    }
}
