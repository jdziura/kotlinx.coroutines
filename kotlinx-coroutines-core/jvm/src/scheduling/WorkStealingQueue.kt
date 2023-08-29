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

internal class WorkStealingQueue(
    val owner: CoroutineScheduler.Worker
) {
    companion object {
        private const val INITIAL_SIZE = 32
    }

    @Volatile
    var array = AtomicReferenceArray(arrayOfNulls<Task?>(INITIAL_SIZE))

    @Volatile
    private var mask = INITIAL_SIZE - 1
    @Volatile
    private var headIndex = 0
    @Volatile
    private var tailIndex = 0


    private val mutex = ReentrantLock()
    private val spinlock = Spinlock()

    val canSteal: Boolean get() = headIndex < tailIndex

    fun localPush(task: Task) {
        var tail = tailIndex

        // [TODO] Reset indexes if tail overflows

        if (tail < headIndex + mask) {
            array.set(tail and mask, task)
            tailIndex = tail + 1
        } else {
            try {
                if (USE_DOTNET_QUEUE_SPINLOCK) spinlock.lock() else mutex.lock()
                val head = headIndex
                val count = tailIndex - headIndex

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

                array.set(tail and mask, task)
                tailIndex = tail + 1
            } finally {
                if (USE_DOTNET_QUEUE_SPINLOCK) spinlock.unlock() else mutex.unlock()
            }
        }
    }

    fun localPop(): Task? = if (headIndex < tailIndex) localPopCore() else null

    private fun localPopCore(): Task? {
        while (true) {
            var tail = tailIndex
            if (headIndex >= tail) return null

            tail--
            tailIndex = tail

            if (headIndex <= tail) {
                val idx = tail and mask
                return array.getAndSet(idx, null) ?: continue
            } else {
                return try {
                    if (USE_DOTNET_QUEUE_SPINLOCK) spinlock.lock() else mutex.lock()
                    if (headIndex <= tail) {
                        val idx = tail and mask
                        array.getAndSet(idx, null) ?: continue
                    } else {
                        tailIndex = tail + 1
                        null
                    }
                } finally {
                    if (USE_DOTNET_QUEUE_SPINLOCK) spinlock.unlock() else mutex.unlock()
                }
            }
        }
    }

    fun trySteal(missedSteal: ObjectRef<Boolean>): Task? {
        while (true) {
            if (canSteal) {
                missedSteal.element = true
                var lockTaken = false
                try {
                    lockTaken = if (USE_DOTNET_QUEUE_SPINLOCK) spinlock.tryLock() else mutex.tryLock()
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
                        if (USE_DOTNET_QUEUE_SPINLOCK) spinlock.unlock() else mutex.unlock()
                    }
                }
            }

            return null
        }
    }
}