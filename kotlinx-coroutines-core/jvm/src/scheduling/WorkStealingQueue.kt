/*
 * Copyright 2016-2023 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.scheduling

import kotlinx.atomicfu.*
import kotlinx.coroutines.internal.ReentrantLock
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.jvm.internal.Ref.ObjectRef

internal interface DotnetQueueLock {
    fun lock()
    fun tryLock(): Boolean
    fun unlock()
}

internal class DotnetQueueSpinLock : DotnetQueueLock {
    companion object {
        private const val SPIN_COUNT = 64
    }

    private val flag = atomic(false)

    override fun lock() {
        while (true) {
            repeat(SPIN_COUNT) {
                if (flag.compareAndSet(false, true)) return
            }

            Thread.yield()
        }
    }

    override fun tryLock(): Boolean = flag.compareAndSet(false, true)

    override fun unlock() {
        flag.value = false
    }
}

internal class DotnetQueueReentrantLock : DotnetQueueLock {
    private val mutex = ReentrantLock()
    override fun lock() = mutex.lock()
    override fun tryLock() = mutex.tryLock()
    override fun unlock() = mutex.unlock()
}

internal class WorkStealingQueue(
    private val delayable: Boolean
) {
    companion object {
        private const val INITIAL_SIZE = 32
    }

    @Volatile var array = arrayOfNulls<Task?>(INITIAL_SIZE)

    @Volatile private var mask = INITIAL_SIZE - 1
    @Volatile private var headIndex = 0
    @Volatile private var tailIndex = 0

    private val mutex: DotnetQueueLock =
        if (USE_DOTNET_QUEUE_SPINLOCK) DotnetQueueSpinLock()
        else DotnetQueueReentrantLock()

    private val canSteal: Boolean get() = headIndex < tailIndex

    val size: Int get() = tailIndex - headIndex

    fun add(task: Task) {
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

    fun poll(): Task? {
        while (true) {
            var tail = tailIndex
            if (headIndex >= tail) {
                if (delayable) {
                    // In delayable version, we have to adjust an implementation because of following scenario:
                    // Another thread tries to steal the only task in queue and increases head early with a lock taken.
                    // However, the task is not stealable yet, thus it reverts changes and exits. If the owner thread
                    // is at this point at the code during this time, it assumes that either there are no tasks, or
                    // a stealer will successfully dequeue the only task. Since it may not happen, we have to acquire
                    // a lock and check again
                    try {
                        mutex.lock()

                        tail = tailIndex - 1

                        if (headIndex <= tail) {
                            tailIndex = tail
                            val idx = tail and mask
                            val task = array[idx]
                            array[idx] = null
                            return task
                        }

                        // There actually are no tasks in queue, return.
                        return null
                    } finally {
                        mutex.unlock()
                    }
                } else {
                    // Queue is definitely empty, return.
                    return null
                }
            }

            // Decrease tail early to stop from concurrent stealing.
            tail--
            tailIndex = tail

            if (headIndex <= tail) {
                // No race should be possible. We can retrieve stolen element.
                val idx = tail and mask
                val task = array[idx]
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

    fun trySteal(stolenTaskRef: ObjectRef<Task?>): Long {
        var missedSteal = false

        while (canSteal) {
            var lockTaken = false
            try {
                lockTaken = mutex.tryLock()

                if (!lockTaken) {
                    // There is another thread stealing/popping now, we don't want to interfere.
                    missedSteal = true
                    break
                }

                // Increase head early to stop from concurrent local pop.
                val head = headIndex
                headIndex = head + 1

                val size = tailIndex - head

                if (size > 0) {
                    // No race should be possible. We can retrieve stolen element.
                    val idx = head and mask
                    val task = array[idx]

                    if (delayable && size == 1) {
                        // We want to steal last scheduled task. If we care about delay, we need to
                        // check if sufficient time has passed since submission.
                        val time = schedulerTimeSource.nanoTime()
                        require(task != null)

                        val staleness = time - task.submissionTime
                        if (staleness < WORK_STEALING_TIME_RESOLUTION_NS) {
                            // We are not stealing and have to restore the head
                            headIndex = head
                            return WORK_STEALING_TIME_RESOLUTION_NS - staleness
                        }
                    }

                    stolenTaskRef.element = array[idx]
                    array[idx] = null
                    return TASK_STOLEN
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
            MISSED_STEAL
        } else {
            NOTHING_TO_STEAL
        }
    }
}

internal interface LocalQueue {
    fun add(task: Task, tailDispatch: Boolean = false): Task?
    fun poll(): Task?
    fun trySteal(stolenTaskRef: ObjectRef<Task?>): Long
}

internal class LocalQueueKotlin(
    delayable: Boolean = true
) : LocalQueue {
    val queue = WorkQueue(delayable)
    override fun add(task: Task, tailDispatch: Boolean) = queue.add(task, tailDispatch)
    override fun poll(): Task? = queue.poll()
    override fun trySteal(stolenTaskRef: ObjectRef<Task?>) = queue.trySteal(STEAL_ANY, stolenTaskRef)
}

internal class LocalQueueDotnet(
    delayable: Boolean = false
) : LocalQueue {
    val queue = WorkStealingQueue(delayable)
    override fun add(task: Task, tailDispatch: Boolean): Task? {
        queue.add(task)
        return null
    }
    override fun poll(): Task? = queue.poll()
    override fun trySteal(stolenTaskRef: ObjectRef<Task?>) = queue.trySteal(stolenTaskRef)
}

internal class LocalConcurrentLinkedQueue : LocalQueue {
    val queue = ConcurrentLinkedQueue<Task>()
    override fun add(task: Task, tailDispatch: Boolean) = if (queue.add(task)) null else task
    override fun poll(): Task? = queue.poll()
    override fun trySteal(stolenTaskRef: ObjectRef<Task?>): Long {
        val task = queue.poll() ?: return NOTHING_TO_STEAL
        stolenTaskRef.element = task
        return TASK_STOLEN
    }
}

// poll / steal from different sides of the queue
internal class LocalConcurrentLinkedDeque : LocalQueue {
    val queue = ConcurrentLinkedDeque<Task>()
    override fun add(task: Task, tailDispatch: Boolean) = null.also { queue.addLast(task) }
    override fun poll(): Task? = queue.pollLast()
    override fun trySteal(stolenTaskRef: ObjectRef<Task?>): Long {
        val task = queue.pollFirst() ?: return NOTHING_TO_STEAL
        stolenTaskRef.element = task
        return TASK_STOLEN
    }
}