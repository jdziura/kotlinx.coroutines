/*
 * Copyright 2016-2021 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.scheduling

import kotlinx.atomicfu.*
import kotlinx.atomicfu.AtomicBoolean
import kotlinx.coroutines.internal.*
import kotlinx.coroutines.trackTask
import kotlinx.coroutines.unTrackTask
import java.io.*
import java.lang.Runnable
import java.util.*
import java.util.concurrent.*
import java.util.concurrent.atomic.*
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.LockSupport.*
import java.util.concurrent.locks.ReentrantLock
import kotlin.collections.ArrayDeque
import kotlin.random.*
import kotlin.random.Random

internal const val NUM_PROCESSING_WORK_SHIFT = 0
internal const val NUM_EXISTING_THREADS_SHIFT = 16
internal const val NUM_THREADS_GOAL_SHIFT = 32

internal class CsBasedCoroutineScheduler(
    @JvmField val nProcessors: Int,
    @JvmField val maxWorkers: Int,
    @JvmField val schedulerName: String = DEFAULT_SCHEDULER_NAME
) : Scheduler {
    init {
        require(nProcessors >= CoroutineScheduler.MIN_SUPPORTED_POOL_SIZE) {
            "Core pool size $nProcessors should be at least ${CoroutineScheduler.MIN_SUPPORTED_POOL_SIZE}"
        }
        require(maxWorkers >= nProcessors) {
            "Max pool size $maxWorkers should be greater than or equals to core pool size $nProcessors"
        }
        require(maxWorkers <= CoroutineScheduler.MAX_SUPPORTED_POOL_SIZE) {
            "Max pool size $maxWorkers should not exceed maximal supported number of threads ${CoroutineScheduler.MAX_SUPPORTED_POOL_SIZE}"
        }
    }

    val workQueue = CsBasedWorkQueue(this)
    val numRequestedWorkers = atomic(0)

    internal class ThreadCounts(_data: Long) {
        val data = atomic(_data)

        // Not atomic
        private fun setValue(value: Long, shift: Int) {
            var newData = data.value
            val allSet: Long = UShort.MAX_VALUE as Long
            val clearBytes: Long = (allSet shl shift).inv()
            newData = (newData and clearBytes) or (value shl shift)
            data.value = newData
        }

        var numProcessingWork: Short
            get() = (data.value shr NUM_PROCESSING_WORK_SHIFT) as Short
            set(value) = setValue(value as Long, NUM_PROCESSING_WORK_SHIFT)

        var numExistingThreads: Short
            get() = (data.value shr NUM_EXISTING_THREADS_SHIFT) as Short
            set(value) = setValue(value as Long, NUM_EXISTING_THREADS_SHIFT)

        var numThreadsGoal: Short
            get() = (data.value shr NUM_THREADS_GOAL_SHIFT) as Short
            set(value) = setValue(value as Long, NUM_THREADS_GOAL_SHIFT)
    }

    val counts = ThreadCounts(0L)

    fun requestWorker() {
        numRequestedWorkers.incrementAndGet()

    }

    override fun dispatch(block: Runnable, taskContext: TaskContext, tailDispatch: Boolean) {
        trackTask()
        val task = createTask(block, taskContext)
        workQueue.enqueue(task, true)
    }

    override fun createTask(block: Runnable, taskContext: TaskContext): Task {
        val nanoTime = schedulerTimeSource.nanoTime()
        if (block is Task) {
            block.submissionTime = nanoTime
            block.taskContext = taskContext
            return block
        }
        return TaskImpl(block, nanoTime, taskContext)
    }

    internal object WorkerThread {
        fun maybeAddWorkingWorker() {
//            val oldCounts = counts.value
//            while (true) {
//                val numProcessingWork = getProcessingWork(oldCounts)
//                if (numProcessingWork >= getThreadsGoal(oldCounts)) {
//                    return
//                }
//
//                val newNumProcessingWork = numProcessingWork + 1
//                val numExistingThreads = getExistingThreads(oldCounts)
//                val newNumExistingThreads = max(numExistingThreads, newNumProcessingWork)
//
//                val newCounts = oldCounts
//
//            }
        }

        fun workerStart() {
            // TODO - implement
//            val curCounts = counts.value
//            while (true) {
//                val numProcessingWork
//            }

        }
        fun createWorkerThread() {
            // TODO - implement
        }
    }
//    internal inner class Worker private constructor() : Thread() {
//        init {
//            isDaemon = true
//        }
//
//        var indexInArray = 0
//            set(index) {
//                name = "$schedulerName-worker-$index"
//                field = index
//            }
//
//        constructor(index: Int) : this() {
//            indexInArray = index
//        }
//        inline val scheduler get() = this@CsBasedCoroutineScheduler
//
//        override fun run() = runWorker()
//
//        private fun runWorker() {}
//    }

    fun runSafely(task: Task) {
        try {
            task.run()
        } catch (e: Throwable) {
            val thread = Thread.currentThread()
            thread.uncaughtExceptionHandler.uncaughtException(thread, e)
        } finally {
            unTrackTask()
        }
    }

    override fun execute(command: Runnable) = dispatch(command)

    override fun close() {
        shutdown(10_000L)
    }

    override fun shutdown(timeout: Long) {

    }
}