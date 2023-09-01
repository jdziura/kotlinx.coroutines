/*
 * Copyright 2016-2023 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.scheduling

import kotlinx.coroutines.channels.*
import kotlinx.coroutines.internal.ReentrantLock
import java.util.concurrent.atomic.*

internal class Spinlock {
    private val flag = AtomicInteger(0)
    fun lock() {
        while (!flag.compareAndSet(0, 1)) {}
    }

    fun tryLock(): Boolean {
        return flag.compareAndSet(0, 1)
    }

    fun unlock() {
        flag.set(0)
    }
}

internal class QueueLock {
    private val spinLock = Spinlock()
    private val reentrantLock = ReentrantLock()
    fun lock() = if (USE_DOTNET_QUEUE_SPINLOCK) spinLock.lock() else reentrantLock.lock()
    fun tryLock() = if (USE_DOTNET_QUEUE_SPINLOCK) spinLock.tryLock() else reentrantLock.tryLock()
    fun unlock() = if (USE_DOTNET_QUEUE_SPINLOCK) spinLock.unlock() else reentrantLock.unlock()
}

internal class WorkStealingQueue {
    companion object {
        private const val INITIAL_SIZE = 32
    }

    @Volatile var array = AtomicReferenceArray(arrayOfNulls<Task?>(INITIAL_SIZE))

    @Volatile private var mask = INITIAL_SIZE - 1
    @Volatile private var headIndex = 0
    @Volatile private var tailIndex = 0

    private val mutex = QueueLock()
    private val canSteal: Boolean get() = headIndex < tailIndex

    fun localPush(task: Task) {
        var tail = tailIndex

        if (tail == Int.MAX_VALUE) {
            // We need to reset indexes. By masking off the unnecessary bits instead of setting to 0,
            // we don't need to reorder elements in queue.
            try {
                mutex.lock()

                require(tailIndex == Int.MAX_VALUE)

                headIndex = headIndex and mask
                tail = tailIndex and mask
                tailIndex = tail

                require(headIndex <= tailIndex)
            } finally {
                mutex.unlock()
            }
        }

        if (tail < headIndex + mask) {
            // This means that there are at least 2 free elements in array, so we can add new task without resizing.
            array.set(tail and mask, task)
            tailIndex = tail + 1
        }
        else {
            try {
                mutex.lock()

                val head = headIndex
                val count = tailIndex - headIndex

                if (count >= mask) {
                    // We need to resize array.
                    val newArray = AtomicReferenceArray(arrayOfNulls<Task?>(array.length() * 2))
                    for (i in 0 until array.length()) {
                        newArray[i] = array.get((i + head) and mask)
                    }

                    array = newArray
                    headIndex = 0
                    tail = count
                    tailIndex = tail
                    mask = (mask * 2) + 1
                }

                // Finally, add element.
                array.set(tail and mask, task)
                tailIndex = tail + 1
            } finally {
                mutex.unlock()
            }
        }
    }

    fun localPop(): Task? {
        while (true) {
            var tail = tailIndex
            if (headIndex >= tail) {
                // No tasks in queue, return.
                return null
            }

            // Decrease tail beforehand to stop from concurrent stealing.
            tail--
            tailIndex = tail

            if (headIndex <= tail) {
                // At this point we know that stealing will not interfere
                // and can retrieve element without taking lock.
                // Note: I'm not certain if this claim is true, it should be for correctness.
                val idx = tail and mask
                val task = array.get(idx) ?: continue // [TODO] Check when this happens.
                array.set(idx, null)
                return task
            } else {
                // We may be racing with stealing, need to synchronize with a lock.
                try {
                    mutex.lock()

                    if (headIndex <= tail) {
                        // We won a race and can retrieve popped element.
                        val idx = tail and mask
                        val task = array.get(idx) ?: continue // [TODO] Check when this happens.
                        array.set(idx, null)
                        return task
                    } else {
                        // We lost the race and have to restore the tail.
                        tailIndex = tail + 1
                        return null // [TODO] Maybe need to continue instead?
                    }
                } finally {
                    mutex.unlock()
                }
            }
        }
    }

    fun trySteal(): ChannelResult<Task?> {
        var missedSteal = false

        while (canSteal) {
            var lockTaken = false
            try {
                // We only try to lock, to not interfere with concurrent steals and local pop.
                lockTaken = mutex.tryLock()

                if (!lockTaken) {
                    missedSteal = true
                    continue
                }

                // Increase head beforehand to stop from concurrent local pop
                val head = headIndex
                headIndex = head + 1

                if (head < tailIndex) {
                    // No race should be possible. We can retrieve stolen element.
                    val idx = head and mask
                    val task = array.get(idx) ?: continue // [TODO] Check when this happens.
                    array.set(idx, null)
                    return ChannelResult.success(task)
                } else {
                    // We lost a race and have to restore the head.
                    headIndex = head
                    missedSteal = true
                }
            } finally {
                if (lockTaken) {
                    mutex.unlock()
                }
            }
        }

        // We didn't succeed.
        // If we did miss steal though, we need to notify it with ChannelResult.failure(), since
        // there might be additional work to do in this queue and additional
        // thread may have to be woken up to process it.
        return if (missedSteal) {
            ChannelResult.failure()
        } else {
            ChannelResult.success(null)
        }
    }
}