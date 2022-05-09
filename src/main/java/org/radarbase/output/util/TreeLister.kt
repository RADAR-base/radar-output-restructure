package org.radarbase.output.util

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedReceiveChannelException
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch

class TreeLister<T, C>(
    private val levelLister: LevelLister<T, C>,
) {
    suspend fun list(
        context: C,
        limit: Int = Int.MAX_VALUE,
        predicate: ((T) -> Boolean)? = null
    ): List<T> = listTo(mutableListOf(), context, limit, predicate)

    suspend fun <S : MutableCollection<T>> listTo(
        collection: S,
        context: C,
        limit: Int = Int.MAX_VALUE,
        predicate: ((T) -> Boolean)? = null
    ): S = coroutineScope {
        val channel = Channel<T>(capacity = limit)
        val producer = launch {
            coroutineScope {
                descend(
                    context,
                    if (predicate == null) channel::send else ({ value -> if (predicate(value)) channel.send(value) }),
                )
            }
            channel.close()
        }

        try {
            repeat(limit) {
                collection += channel.receive()
            }
            producer.cancel()
        } catch (ex: ClosedReceiveChannelException) {
            // done: channel closed by producer
        }

        collection
    }

    private fun CoroutineScope.descend(context: C, emit: suspend (T) -> Unit) {
        levelLister.run {
            listLevel(context, { descend(it, emit) }, emit)
        }
    }

    interface LevelLister<T, C> {
        fun CoroutineScope.listLevel(
            context: C,
            descend: CoroutineScope.(C) -> Unit,
            emit: suspend (T) -> Unit
        ): Job
    }
}
