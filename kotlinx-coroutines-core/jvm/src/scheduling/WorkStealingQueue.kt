/*
 * Copyright 2016-2023 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.scheduling

import kotlinx.atomicfu.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.internal.ReentrantLock

internal class Spinlock {
    private val flag = atomic(false)
    fun lock() {
        while (!flag.compareAndSet(false, true)) {}
    }

    fun tryLock(): Boolean {
        return flag.compareAndSet(false, true)
    }

    fun unlock() {
        flag.value = false
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

    @Volatile var array = arrayOfNulls<Task?>(INITIAL_SIZE)

    @Volatile private var mask = INITIAL_SIZE - 1
    @Volatile private var headIndex = 0
    @Volatile private var tailIndex = 0

    private val mutex = QueueLock()
    private val canSteal: Boolean get() = headIndex < tailIndex

    fun localPush(task: Task) {
        var tail = tailIndex

        if (tail == Int.MAX_VALUE) {
            // We need to reset indexes to prevent integer overflow.
            try {
                mutex.lock()

                require(tailIndex == Int.MAX_VALUE)

                // By masking off the unnecessary bits instead of setting to 0,
                // we don't need to reorder elements in queue.
                headIndex = headIndex and mask
                tail = tailIndex and mask
                tailIndex = tail

                // Note: condition head <= tail is still valid, since Int.MAX_VALUE has all bits set,
                // so tailIndex masked off becomes max index in array.
                require(headIndex <= tailIndex)
            } finally {
                mutex.unlock()
            }
        }

        if (tail < headIndex + mask) {
            // This means that there are at least 2 free elements in array,
            // so we can add new task without resizing.
            array[tail and mask] = task
            tailIndex = tail + 1
            return
        }

        try {
            // We may need to resize the array.
            mutex.lock()

            val head = headIndex
            val count = tailIndex - headIndex

            if (count >= mask) {
                // There is no space left, resize array.

                val newArray = arrayOfNulls<Task?>(array.size * 2)
                for (i in 0 until array.size) {
                    newArray[i] = array[(i + head) and mask]
                }

                array = newArray
                headIndex = 0
                tail = count
                tailIndex = tail
                mask = (mask * 2) + 1
            }

            // Finally, add element.
            array[tail and mask] = task
            tailIndex = tail + 1
        } finally {
            mutex.unlock()
        }
    }

    fun localPop(): Task? {
        while (true) {
            var tail = tailIndex
            if (headIndex >= tail) {
                // Queue empty, return.
                return null
            }

            // Decrease tail early to stop from concurrent stealing.
            tail--
            tailIndex = tail

            if (headIndex <= tail) {
                // No race should be possible. We can retrieve stolen element.
                val idx = tail and mask
                val task = array[idx]
                require(task != null)
                array[idx] = null
                return task
            }

            try {
                // We may be racing with stealing: 0 or 1 element left
                mutex.lock()

                if (headIndex <= tail) {
                    // We won a race and can retrieve element
                    val idx = tail and mask
                    val task = array[idx]
                    require(task != null)
                    array[idx] = null
                    return task
                }

                // We lost the race and have to restore the tail.
                tailIndex = tail + 1
                return null
            } finally {
                mutex.unlock()
            }
        }
    }

    // Returns ChannelResult.failure() if we didn't steal and missed.
    // On success, returns null if queue was empty, or stolen task.
    fun trySteal(): ChannelResult<Task?> {
        var missedSteal = false

        while (canSteal) {
            var lockTaken = false
            try {
                lockTaken = mutex.tryLock()

                if (!lockTaken) {
                    // There is another thread stealing/popping now, we don't want to interfere.
                    missedSteal = true
                    continue
                }

                // Increase head early to stop from concurrent local pop.
                val head = headIndex
                headIndex = head + 1

                if (head < tailIndex) {
                    // No race should be possible. We can retrieve stolen element.
                    val idx = head and mask
                    val task = array[idx]
                    require(task != null)
                    array[idx] = null
                    return ChannelResult.success(task)
                }

                // We lost the race and have to restore the head.
                headIndex = head
                missedSteal = true
            } finally {
                if (lockTaken) mutex.unlock()
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