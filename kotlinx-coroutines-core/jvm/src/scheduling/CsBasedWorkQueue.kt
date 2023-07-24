/*
 * Copyright 2016-2023 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.scheduling

import kotlinx.coroutines.*
import java.util.concurrent.atomic.*
import java.util.concurrent.locks.*
import java.util.concurrent.*
import kotlinx.atomicfu.*

// SA - AllThreadQueues
// Q - WorkStealingQueue
internal const val SA_INITIAL_SIZE = 16
internal const val Q_INITIAL_SIZE = 32
internal const val Q_START_INDEX = 0

internal class WorkQueueThreadLocals(
    val wq: CsBasedWorkQueue
) {
    companion object {
        @JvmStatic
        val threadLocals = ThreadLocal<WorkQueueThreadLocals?>()
    }

//    var isProcessingHighPriorityWorkItems: Boolean = false
    var queueIndex: Int = 0
    val assignedGlobalWorkItemQueue = wq.workItems
    val workQueue = wq
    val workStealingQueue = CsBasedWorkQueue.WorkStealingQueue()
//    val currentThread = Thread.currentThread()
//    val threadLocalCompletionCountObject = CsBasedCoroutineScheduler.getOrCreateThreadLocalCompletionCountObject()
//    val random = Random.XoshiroImpl()

    init {
        CsBasedWorkQueue.WorkStealingQueueList.add(workStealingQueue)
        threadLocals.set(this)
    }

    fun transferLocalWork() {
        while (true) {
            val task = workStealingQueue.localPop()
            if (task != null) {
                workQueue.enqueue(task, true)
            } else {
                break
            }
        }
    }

    protected fun finalize() {
        transferLocalWork()
        CsBasedWorkQueue.WorkStealingQueueList.remove(workStealingQueue)
    }
}
internal class CsBasedWorkQueue(
    val _scheduler: CsBasedCoroutineScheduler
) {
    internal object WorkStealingQueueList {
        private var queues = emptyArray<WorkStealingQueue?>()

        fun add(q: WorkStealingQueue) {
            synchronized(this) {
//                require(queues != null)
                require(queues.indexOf(q) < 0)

                val newQueues = queues.copyOf(queues.size + 1)
                newQueues[newQueues.lastIndex] = q
                queues = newQueues
            }
        }

        fun remove(q: WorkStealingQueue) {
            synchronized(this) {
//                require(queues != null)
                if (queues.size == 0) {
                    return
                }

                val pos = queues.indexOf(q)
                require(pos >= 0)

                val newQueues = Array(queues.size - 1) { i ->
                    if (i < pos) {
                        queues[i]
                    } else {
                        queues[i + 1]
                    }
                }

                queues = newQueues
            }
        }
    }

    internal class WorkStealingQueue {
        private var array = arrayOfNulls<Task?>(Q_INITIAL_SIZE)
        private var mask = Q_INITIAL_SIZE - 1
        private var headIndex = Q_START_INDEX
        private var tailIndex = Q_START_INDEX

        fun localPush(task: Task) {
            synchronized(this) {
                // TODO - check if needed
                if (tailIndex == Int.MAX_VALUE) {
                    headIndex = headIndex and mask
                    tailIndex = tailIndex and mask
                    require(headIndex <= tailIndex)
                }

                if (tailIndex < headIndex + mask) {
                    array[tailIndex and mask] = task
                    tailIndex++
                } else {
                    val count = tailIndex - headIndex
                    if (count >= mask) {
                        val newArray = arrayOfNulls<Task?>(2 * array.size)
                        for (i in array.indices) {
                            newArray[i] = array[(i + headIndex) and mask]
                        }

                        array = newArray
                        headIndex = 0
                        tailIndex = count
                        mask = 2 * mask + 1
                    }

                    array[tailIndex and mask] = task
                    tailIndex++
                }
            }
        }

        fun localFindAndPop(task: Task): Boolean {
            synchronized(this) {
                if (array[(tailIndex - 1) and mask] == task) {
                    val unused = localPop()
                    if (unused != null) {
                        require(unused == task)
                        return true
                    } else {
                        return false
                    }
                }

                for (i in tailIndex downTo headIndex) {
                    if (array[i and mask] == task) {
                        array[i and mask] = null
                        if (i == tailIndex) {
                            tailIndex--
                        } else if (i == headIndex) {
                            headIndex++
                        }
                        return true
                    }
                }

                return false
            }
        }

        fun localPop(): Task? {
            synchronized(this) {
                while (true) {
                    if (headIndex >= tailIndex) {
                        return  null
                    }

                    val tail = tailIndex
                    tailIndex--

                    if (headIndex <= tail) {
                        val idx = tail and mask
                        var task = array[idx]
                        if (task == null) {
                            continue
                        }

                        array[idx] = null
                        return task
                    } else {
                        tailIndex = tail + 1
                        return null
                    }
                }
            }
        }
    }

    val workItems = ConcurrentLinkedQueue<Task>()
    val processors = Runtime.getRuntime().availableProcessors()
    val processorsPerAssignableWorkItemQueue = 16
    val assignableWorkItemQueueCount: Int
        get() = if (processors <= 32) 0
                else (processors + processorsPerAssignableWorkItemQueue - 1) / processorsPerAssignableWorkItemQueue

    var hasOutstandingThreadRequest = atomic(0)

    val scheduler: CsBasedCoroutineScheduler = _scheduler

    fun ensureThreadRequested() {
        System.err.println("ensureThreadRequested")
        if (hasOutstandingThreadRequest.compareAndSet(0, 1)) {
            scheduler.requestWorker()
        }
    }
    fun enqueue(task: Task, forceGlobal: Boolean) {
        synchronized(this) {
            System.err.println("enqueue")
            val t1 = WorkQueueThreadLocals.threadLocals.get()

            if (forceGlobal == false && t1 != null) {
                t1.workStealingQueue.localPush(task)
            } else {
                val queue =
                    if (assignableWorkItemQueueCount > 0 && t1 != null)
                        t1.assignedGlobalWorkItemQueue
                    else
                        workItems
                queue.add(task)
            }

            ensureThreadRequested()
        }
    }
}