package org.radarbase.output.accounting

import com.almworks.integers.LongArray
import org.apache.jute.Index
import java.time.Instant

class OffsetIntervals {
    private val offsetsFrom: LongArray
    private val offsetsTo: LongArray
    private val lastProcessed: MutableList<Instant>

    constructor() {
        offsetsFrom = LongArray(8)
        offsetsTo = LongArray(8)
        lastProcessed = ArrayList(8)
    }

    constructor(other: OffsetIntervals) {
        offsetsFrom = LongArray(other.offsetsFrom)
        offsetsTo = LongArray(other.offsetsTo)
        lastProcessed = ArrayList(other.lastProcessed)
    }

    fun contains(range: OffsetRangeSet.Range): Boolean {
        //  -index-1 if not found
        val searchIndex = offsetsFrom.binarySearch(range.from)
        val index = if (searchIndex >= 0) searchIndex else -searchIndex - 2
        return (index >= 0
                && range.to <= offsetsTo[index]
                && range.lastProcessed <= lastProcessed[index])
    }

    fun contains(offset: Long, lastModified: Instant): Boolean {
        //  -index-1 if not found
        val searchIndex = offsetsFrom.binarySearch(offset)
        if (searchIndex >= 0) {
            return lastModified <= lastProcessed[searchIndex]
        }

        val indexBefore = -searchIndex - 2
        return (indexBefore >= 0
                && offset <= offsetsTo[indexBefore]
                && lastModified <= lastProcessed[indexBefore])
    }

    fun add(offset: Long, lastModified: Instant) {
        var index = offsetsFrom.binarySearch(offset)
        if (index >= 0) {
            lastProcessed[index] = max(lastProcessed[index], lastModified)
            return
        }
        // index where this range would be entered
        index = -index - 1

        if (index > 0 && offset == offsetsTo[index - 1] + 1) {
            // concat with range before it and possibly stream afterwards
            offsetsTo[index - 1] = offset
            lastProcessed[index - 1] = max(lastProcessed[index - 1], lastModified)
            if (index < offsetsFrom.size() && offset == offsetsFrom[index] - 1) {
                offsetsTo[index - 1] = offsetsTo[index]
                lastProcessed[index - 1] = max(lastProcessed[index - 1], lastProcessed[index])
                removeAt(index)
            }
        } else if (index >= offsetsFrom.size() || offset < offsetsFrom[index] - 1) {
            // cannot concat, enter new range
            insert(index, offset, offset, lastModified)
        } else {
            // concat with the stream after it
            offsetsFrom[index] = offset
            lastProcessed[index] = max(lastProcessed[index], lastModified)
        }
    }

    fun add(range: OffsetRangeSet.Range) {
        val (from, to, lastModified) = range
        var index = offsetsFrom.binarySearch(from)
        if (index < 0) {
            // index where this range would be entered
            index = -index - 1

            if (index > 0 && from <= offsetsTo[index - 1] + 1) {
                // concat with range before it and possibly stream afterwards
                index--
                lastProcessed[index] = max(lastProcessed[index], lastModified)
            } else if (index >= offsetsFrom.size() || to < offsetsFrom[index] - 1) {
                // cannot concat, enter new range
                insert(index, from, to, lastModified)
                return
            } else {
                // concat with the stream after it
                offsetsFrom[index] = from
                lastProcessed[index] = max(lastProcessed[index], lastModified)
            }
        } else {
            lastProcessed[index] = max(lastProcessed[index], lastModified)
        }

        if (to <= offsetsTo[index]) {
            return
        }

        offsetsTo[index] = to

        var overlapIndex = index + 1
        val startIndex = overlapIndex
        var maxProcessed = lastProcessed[index]

        while (overlapIndex < offsetsTo.size() && to >= offsetsTo[overlapIndex]) {
            maxProcessed = max(maxProcessed, lastProcessed[overlapIndex])
            overlapIndex++
        }
        if (overlapIndex < offsetsFrom.size() && to >= offsetsFrom[overlapIndex] - 1) {
            offsetsTo[index] = offsetsTo[overlapIndex]
            maxProcessed = max(maxProcessed, lastProcessed[overlapIndex])
            overlapIndex++
        }
        if (overlapIndex != startIndex) {
            removeRange(startIndex, overlapIndex)
            lastProcessed[index] = maxProcessed
        }
    }

    fun forEach(
            action: (offsetFrom: Long, offsetTo: Long, lastModified: Instant) -> Unit
    ) = repeat(lastProcessed.size) { i ->
        action(offsetsFrom[i], offsetsTo[i], lastProcessed[i])
    }

    fun toList(): List<OffsetRangeSet.Range> = List(lastProcessed.size) { i ->
        OffsetRangeSet.Range(offsetsFrom[i], offsetsTo[i], lastProcessed[i])
    }

    fun size(): Int = offsetsFrom.size()

    override fun toString(): String {
        return ("[" + lastProcessed.indices.joinToString(", ") { i ->
            "(${offsetsFrom[i]} - ${offsetsTo[i]}, ${lastProcessed[i]})"
        } + "]")
    }

    fun remove(range: OffsetRangeSet.Range) {
        val (from, to, _) = range

        var index = offsetsFrom.binarySearch(from)

        if (index < 0) {  // search comes between -index - 2 and -index - 1
            index = -index - 1 // is the next stored offsetFrom index after from
            // There is a previous from index. Check for overlap
            if (index > 0) {
                val prevIndex = index - 1
                if (from <= offsetsTo[prevIndex]) {  // there is overlap
                    if (to < offsetsTo[prevIndex]) { // the range falls inside an existing interval
                        // create new interval after the removed range. The interval before the
                        // range remains.
                        insert(index, to + 1, offsetsTo[prevIndex], lastProcessed[prevIndex])
                    }

                    offsetsTo[prevIndex] = from - 1
                }
            }
        }

        // remove intervals inside range
        while (offsetsFrom[index] >= from && offsetsTo[index] <= to) {
            removeAt(index)
            if (index == offsetsTo.size()) {  // last interval has been removed
                return
            }
        }

        // last interval not completely overlapping with range
        if (offsetsFrom[index] <= to) {
            offsetsFrom[index] = to + 1
        }
    }

    private fun insert(index: Int, from: Long, to: Long, lastModified: Instant) {
        offsetsFrom.insert(index, from)
        offsetsTo.insert(index, to)
        if (index >= lastProcessed.size) {
            lastProcessed.add(lastModified)
        } else {
            lastProcessed.add(index, lastModified)
        }
    }

    private fun removeAt(index: Int) {
        offsetsTo.removeAt(index)
        offsetsFrom.removeAt(index)
        lastProcessed.removeAt(index)
    }

    private fun removeRange(from: Int, to: Int) {
        offsetsFrom.removeRange(from, to)
        offsetsTo.removeRange(from, to)
        lastProcessed.subList(from, to).clear()
    }

    companion object {
        private fun <T: Comparable<T>> max(a: T, b: T): T = if (a >= b) a else b
    }
}
