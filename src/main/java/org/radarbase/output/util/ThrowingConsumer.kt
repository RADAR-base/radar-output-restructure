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

import java.io.IOException
import java.io.UncheckedIOException
import java.util.function.Consumer

/** Consumer that throws IOException.  */
object ThrowingConsumer {
    /** Regularizes throwing consumer by catching the exception.  */
    fun <T> tryCatch(consumer: (T) -> Unit, catchClause: (T, IOException) -> Unit): Consumer<T> {
        return Consumer { t ->
            try {
                consumer(t)
            } catch (ex: IOException) {
                catchClause(t, ex)
            }
        }
    }

    /** Regularizes throwing consumer by rethrowing IOException as UncheckedIOException.  */
    fun <T> tryCatch(consumer: (T) -> Unit, uncheckedIo: String): Consumer<T> {
        return tryCatch(consumer) { _, ex -> throw UncheckedIOException(uncheckedIo, ex) }
    }
}
