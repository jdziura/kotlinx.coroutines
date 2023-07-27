/*
 * Copyright 2016-2023 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.scheduling

// TODO - compress all data to one 64-bit value and use atomic operations
internal class ThreadCounts(
    var numProcessingWork: Int,
    var numExistingThreads: Int,
    var numThreadsGoal: Int
) {
    constructor() : this(0, 0, 1)

    fun copy(): ThreadCounts {
        synchronized(schedulerMonitor) {
            return ThreadCounts(numProcessingWork, numExistingThreads, numThreadsGoal)
        }
    }

    fun compareAndSet(oldCounts: ThreadCounts, newCounts: ThreadCounts): Boolean {
        synchronized(schedulerMonitor) {
            if (this == oldCounts) {
                numProcessingWork = newCounts.numProcessingWork
                numExistingThreads = newCounts.numExistingThreads
                numThreadsGoal = newCounts.numThreadsGoal
                return true
            }
            return false
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as ThreadCounts

        if (numProcessingWork != other.numProcessingWork) return false
        if (numExistingThreads != other.numExistingThreads) return false
        if (numThreadsGoal != other.numThreadsGoal) return false

        return true
    }
}