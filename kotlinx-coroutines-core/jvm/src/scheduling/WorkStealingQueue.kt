/*
 * Copyright 2016-2023 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.scheduling

import kotlin.jvm.internal.Ref.ObjectRef

internal class WorkStealingQueue(
    val owner: CoroutineScheduler.Worker
) {
    companion object {
        private const val INITIAL_SIZE = 32
    }

    var array = arrayOfNulls<Task?>(INITIAL_SIZE)

    private var mask = INITIAL_SIZE - 1
    private var headIndex = 0
    private var tailIndex = 0

    val canSteal: Boolean get() = synchronized(this) { headIndex < tailIndex }

    fun localPush(task: Task) {
        synchronized(this) {
            var tail = tailIndex

            if (tail == Int.MAX_VALUE) {
                tail = localPushHandleTailOverflow()
            }

            if (tail < headIndex + mask) {
                array[tail and mask] = task
                tailIndex = tail + 1
            } else {
                val head = headIndex
                val count = tailIndex - headIndex

                if (count >= mask) {
                    val newArray = arrayOfNulls<Task?>(array.size * 2)
                    for (i in array.indices) {
                        newArray[i] = array[(i + head) and mask]
                    }

                    array = newArray
                    headIndex = 0
                    tail = count
                    tailIndex = tail
                    mask = (mask * 2) + 1
                }

                array[tail and mask] = task
                tailIndex = tail + 1
            }
        }
    }

    private fun localPushHandleTailOverflow(): Int {
        var tail = tailIndex
        if (tail == Int.MAX_VALUE) {
            headIndex = headIndex and mask
            tail = tailIndex and mask
            tailIndex = tail
            require(headIndex <= tailIndex)
        }
        return tail
    }

    fun localPop(): Task? {
        synchronized(this) {
            return if (headIndex < tailIndex) localPopCore() else null
        }
    }

    private fun localPopCore(): Task? {
        synchronized(this) {
            while (true) {
                var tail = tailIndex
                if (headIndex >= tail) return null

                tail--
                tailIndex = tail

                if (headIndex <= tail) {
                    val idx = tail and mask
                    val task = array[idx] ?: continue
                    array[idx] = null
                    return task
                } else {
                    if (headIndex <= tail) {
                        val idx = tail and mask
                        val task = array[idx] ?: continue
                        array[idx] = null
                        return task
                    } else {
                        tailIndex = tail + 1
                    }
                }
            }
        }
    }

    fun trySteal(missedSteal: ObjectRef<Boolean>): Task? {
        synchronized(this) {
            while (true) {
                if (canSteal) {
                    val head = headIndex
                    headIndex = head + 1

                    if (head < tailIndex) {
                        val idx = head and mask
                        val task = array[idx] ?: continue
                        array[idx] = null
                        return task
                    } else {
                        headIndex = head
                    }

                    missedSteal.element = true
                }

                return null
            }
        }
    }
}