/*
 * Copyright 2016-2023 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.scheduling

import kotlinx.coroutines.*
import kotlin.random.*
import java.util.concurrent.atomic.*
import java.util.concurrent.locks.*
import java.util.concurrent.*
import kotlinx.atomicfu.*

internal const val Q_INITIAL_SIZE = 32

internal class WorkQueueThreadLocals(private val wq: CsBasedWorkQueue) {
    companion object {
        @JvmStatic
        val threadLocals = ThreadLocal<WorkQueueThreadLocals?>()
    }

    val assignedGlobalWorkItemQueue = wq.workItems
    val workQueue = wq
    val workStealingQueue = CsBasedWorkQueue.WorkStealingQueue()
    val random = Random.asJavaRandom()
    val currentThread = Thread.currentThread()

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

internal class CsBasedWorkQueue(private val scheduler: CsBasedCoroutineScheduler) {
    internal object WorkStealingQueueList {
        var queues = emptyArray<WorkStealingQueue?>()

        fun add(q: WorkStealingQueue) {
            synchronized(this) {
                require(queues.indexOf(q) < 0)

                val newQueues = queues.copyOf(queues.size + 1)
                newQueues[newQueues.lastIndex] = q
                queues = newQueues
            }
        }

        fun remove(q: WorkStealingQueue) {
            synchronized(this) {
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
        private var headIndex = 0
        private var tailIndex = 0

        var canSteal: Boolean = false
            get() = headIndex < tailIndex

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

        fun trySteal(): Task? {
            synchronized(this) {
                if (!canSteal) {
                    return null
                }

                while (true) {
                    val head = headIndex
                    headIndex++

                    if (head < tailIndex) {
                        val idx = head and mask
                        val task = array[idx]

                        if (task == null) {
                            continue
                        }

                        array[idx] = null
                        return task
                    }
                    else {
                        // should be never reached
                        require(false)
                        headIndex = head
                    }
                }
            }
        }
    }

    val workItems = ConcurrentLinkedQueue<Task>()
    val processors = Runtime.getRuntime().availableProcessors()
    var dispatchNormalPriorityWorkFirst = true
    var hasOutstandingThreadRequest = 0
    var threadLocalCompletionCountObject = 0

    // TODO - implement or ignore
//    val processorsPerAssignableWorkItemQueue = 16
//    val assignableWorkItemQueueCount: Int
//        get() = if (processors <= 32) 0
//        else (processors + processorsPerAssignableWorkItemQueue - 1) / processorsPerAssignableWorkItemQueue


    fun ensureThreadRequested() {
//        System.err.println("ensureThreadRequested")
//        if (hasOutstandingThreadRequest.compareAndSet(0, 1)) {
//            scheduler.requestWorker()
//        }
        synchronized(this) {
            System.err.println("ensureThreadRequested")
            if (hasOutstandingThreadRequest == 0) {
                hasOutstandingThreadRequest = 1
                scheduler.requestWorker()
            }
        }
    }

    fun enqueue(task: Task, forceGlobal: Boolean) {
        synchronized(this) {
            System.err.println("enqueue")
            val t1 = WorkQueueThreadLocals.threadLocals.get()

            if (forceGlobal == false && t1 != null) {
                t1.workStealingQueue.localPush(task)
            } else {
                workItems.add(task)
            }

            ensureThreadRequested()
        }
    }

    fun dequeue(t1: WorkQueueThreadLocals): Task? {
        synchronized(this) {
            var workItem: Task? = t1.workStealingQueue.localPop()
            if (workItem != null) {
                return workItem
            }

            workItem = workItems.poll()
            if (workItem != null) {
                return workItem
            }

            val localWsq = t1.workStealingQueue
            val queues = WorkStealingQueueList.queues
            var c = queues.size
            require(c > 0)
            val maxIndex = c - 1
            var i = t1.random.nextInt(c)
            while (c > 0) {
                val otherQueue = queues[i]!!
                if (otherQueue != localWsq && otherQueue.canSteal) {
                    workItem = otherQueue.trySteal()

                    if (workItem != null) {
                        return workItem
                    }
                }

                i = if (i < maxIndex) i + 1 else 0
                c--
            }

            return null
        }
    }

    fun createThreadLocals(): WorkQueueThreadLocals {
        require(WorkQueueThreadLocals.threadLocals.get() == null)
        WorkQueueThreadLocals.threadLocals.set(WorkQueueThreadLocals(this))
        return WorkQueueThreadLocals.threadLocals.get()!!
    }

    fun getOrCreateThreadLocals(): WorkQueueThreadLocals {
        return WorkQueueThreadLocals.threadLocals.get() ?: createThreadLocals()
    }

    fun markThreadRequestSatisifed() {
        synchronized(this) {
            hasOutstandingThreadRequest = 0
        }
    }

    fun dispatch(): Boolean {
        synchronized(this) {
            val t1 = getOrCreateThreadLocals()
            markThreadRequestSatisifed()

            var workItem: Task? = null
            do {
                // TODO - check
                if (dispatchNormalPriorityWorkFirst && !t1.workStealingQueue.canSteal) {
//                    dispatchNormalPriorityWorkFirst = !dispatchNormalPriorityWorkFirst
                    workItem = workItems.poll()
                }

                if (workItem == null) {
                    workItem = dequeue(t1)
                    if (workItem == null) {
                        // TODO - maybe needed
//                        ensureThreadRequested()
                        return true
                    }
                }

                ensureThreadRequested()
            } while (false)

            val currentThread = t1.currentThread
            require(currentThread == Thread.currentThread())
            return false
        }
    }
}
