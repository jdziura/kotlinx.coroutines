/*
 * Copyright 2016-2023 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.scheduling

import java.util.concurrent.atomic.*

// TODO - increase bit usage and set public max supported value 2^shift_length
internal class ThreadCounts() {
    companion object {
        private const val SHIFT_LENGTH = 16

        private const val SHIFT_PROCESSING_WORK =   0
        private const val SHIFT_EXISTING_THREADS =  SHIFT_LENGTH
        private const val SHIFT_THREADS_GOAL =      SHIFT_LENGTH * 2

        private const val MASK_PROCESSING_WORK =    (1L shl SHIFT_LENGTH) - 1
        private const val MASK_EXISTING_THREADS =   MASK_PROCESSING_WORK shl SHIFT_EXISTING_THREADS
        private const val MASK_THREADS_GOAL =       MASK_PROCESSING_WORK shl SHIFT_THREADS_GOAL
    }
    constructor(numProcessingWork: Int, numExistingThreads: Int, numThreadsGoal: Int) :
        this((numProcessingWork.toLong() shl SHIFT_PROCESSING_WORK) or
            (numExistingThreads.toLong() shl SHIFT_EXISTING_THREADS) or
            (numThreadsGoal.toLong() shl SHIFT_THREADS_GOAL)
        )

    val data = AtomicLong(0)

    constructor(newData: Long) : this() {
        data.set(newData)
    }
    constructor(numThreadsGoal: Int) : this(0, 0, numThreadsGoal)

    private fun getValue(mask: Long, shift: Int): Int {
        return ((data.get() and mask) shr shift).toInt()
    }

    private fun setValue(value: Int, mask: Long, shift: Int) {
        val cleared = data.get() and mask.inv()
        val shifted = value.toLong() shl shift
        data.set(cleared or shifted)
    }

    var numProcessingWork: Int
        get() = getValue(MASK_PROCESSING_WORK, SHIFT_PROCESSING_WORK)
        set(value) = setValue(value, MASK_PROCESSING_WORK, SHIFT_PROCESSING_WORK)

    var numExistingThreads: Int
        get() = getValue(MASK_EXISTING_THREADS, SHIFT_EXISTING_THREADS)
        set(value) = setValue(value, MASK_EXISTING_THREADS, SHIFT_EXISTING_THREADS)

    var numThreadsGoal: Int
        get() = getValue(MASK_THREADS_GOAL, SHIFT_THREADS_GOAL)
        set(value) = setValue(value, MASK_THREADS_GOAL, SHIFT_THREADS_GOAL)

    fun volatileRead(): ThreadCounts = ThreadCounts(data.get())

    fun compareAndExchange(expectedValue: ThreadCounts, newValue: ThreadCounts): ThreadCounts {
        return ThreadCounts(data.compareAndExchange(expectedValue.data.get(), newValue.data.get()))
    }

    fun setNumThreadsGoal(value: Int): ThreadCounts {
        var counts = volatileRead()
        while (true) {
            val newCounts = counts
            newCounts.numThreadsGoal = value

            val countsBeforeUpdate = compareAndExchange(counts, newCounts)
            if (countsBeforeUpdate == counts) {
                return newCounts
            }

            counts = countsBeforeUpdate
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as ThreadCounts

        return data.get() == other.data.get()
    }
}