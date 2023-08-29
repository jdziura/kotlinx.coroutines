/*
 * Copyright 2016-2023 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.scheduling

import kotlinx.coroutines.internal.ReentrantLock
import java.util.concurrent.atomic.*
import kotlin.jvm.internal.Ref.ObjectRef

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
            // we don't need to reorder elements in queue

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
            // This means that there are at least 2 free elements in array, so we can add new task without resizing

            array.set(tail and mask, task)
            tailIndex = tail + 1
        }
        else {
            try {
                mutex.lock()

                val head = headIndex
                val count = tailIndex - headIndex

                // We need to resize array
                if (count >= mask) {
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

                // Finally, add element
                array.set(tail and mask, task)
                tailIndex = tail + 1
            } finally {
                mutex.unlock()
            }
        }
    }

    private fun localPop(): Task? {
        while (true) {
            var tail = tailIndex
            if (headIndex >= tail) {
                // No tasks in queue, return
                return null
            }

            // Decrease tail beforehand to stop from concurrent stealing
            tail--
            tailIndex = tail

            if (headIndex <= tail) {
                val idx = tail and mask
                return array.getAndSet(idx, null) ?: continue
            } else {
                return try {
                    mutex.lock()
                    if (headIndex <= tail) {
                        val idx = tail and mask
                        array.getAndSet(idx, null) ?: continue
                    } else {
                        tailIndex = tail + 1
                        null
                    }
                } finally {
                    mutex.unlock()
                }
            }
        }
    }

    fun trySteal(missedSteal: ObjectRef<Boolean>): Task? {
        while (true) {
            if (canSteal) {
                var lockTaken = false
                try {
                    lockTaken = mutex.tryLock()
                    if (lockTaken) {
                        val head = headIndex
                        headIndex = head + 1

                        if (head < tailIndex) {
                            val idx = head and mask
                            return array.getAndSet(idx, null) ?: continue
                        } else {
                            headIndex = head
                            missedSteal.element = true
                        }
                    } else {
                        missedSteal.element = true
                    }
                } finally {
                    if (lockTaken) {
                        mutex.unlock()
                    }
                }
            }

            return null
        }
    }
}