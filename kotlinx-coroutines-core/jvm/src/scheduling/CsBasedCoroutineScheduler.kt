/*
 * Copyright 2016-2023 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.scheduling

import kotlinx.atomicfu.*
import kotlinx.coroutines.internal.*
import kotlinx.coroutines.*
import java.io.*
import java.lang.Runnable
import java.util.*
import java.util.concurrent.*
import java.util.concurrent.atomic.*
import java.util.concurrent.locks.*
import java.util.concurrent.locks.LockSupport.*
import kotlin.random.*
import kotlin.math.*

internal val schedulerMonitor = Object()

internal const val SCHED_DEBUG = false
internal fun schedDebug(msg: String) {
    if (SCHED_DEBUG)
        System.err.println(msg)
}

internal fun runSafely(task: Task) {
    try {
        task.run()
    } catch (e: Throwable) {
        val thread = Thread.currentThread()
        thread.uncaughtExceptionHandler.uncaughtException(thread, e)
    } finally {
        unTrackTask()
    }
}

internal class CsBasedCoroutineScheduler(
    @JvmField val corePoolSize: Int,
    @JvmField val maxPoolSize: Int,
    @JvmField val schedulerName: String = DEFAULT_SCHEDULER_NAME
) : Scheduler {
    init {
        require(corePoolSize >= CoroutineScheduler.MIN_SUPPORTED_POOL_SIZE) {
            "Core pool size $corePoolSize should be at least ${CoroutineScheduler.MIN_SUPPORTED_POOL_SIZE}"
        }
        require(maxPoolSize >= corePoolSize) {
            "Max pool size $maxPoolSize should be greater than or equals to core pool size $corePoolSize"
        }
        require(maxPoolSize <= CoroutineScheduler.MAX_SUPPORTED_POOL_SIZE) {
            "Max pool size $maxPoolSize should not exceed maximal supported number of threads ${CoroutineScheduler.MAX_SUPPORTED_POOL_SIZE}"
        }
    }

    internal inner class Worker : Thread() {
        init {
            isDaemon = true
            name = "$schedulerName-worker-${nextThreadId.getAndIncrement()}"
        }

        val inStack = atomic(false)

        inline val scheduler get() = this@CsBasedCoroutineScheduler

        override fun run() = runWorker()

        private fun runWorker() {
            schedDebug("[$name] created")
            while (!isTerminated) {
                doWork()
                if (shouldExitWorker()) {
                    tryPark()
                }
            }
        }

        private fun doWork() {
            schedDebug("[$name] doWork()")
            var alreadyRemovedWorkingWorker = false
            while (takeActiveRequest()) {
                if (!workQueue.dispatch()) {
                    alreadyRemovedWorkingWorker = true
                    break
                }

                if (numRequestedWorkers.value <= 0) {
                    break
                }

                yield()
            }

            if (!alreadyRemovedWorkingWorker) {
                removeWorkingWorker()
            }
        }

        private fun shouldExitWorker(): Boolean {
            synchronized(schedulerMonitor) {
                if (counts.numExistingThreads <= counts.numProcessingWork) {
                    return false
                }

                counts.numExistingThreads--
                counts.numThreadsGoal = max(minThreadsGoal, min(counts.numExistingThreads, counts.numThreadsGoal))

                hillClimber.forceChange(counts.numThreadsGoal, HillClimbing.StateOrTransition.ThreadTimedOut)
                return true
            }
        }

        private fun tryPark() {
            if (inStack.getAndSet(true) == false) {
                synchronized(workerStack) { workerStack.push(this) }
            }

            while (inStack.value == true) {
                if (isTerminated) break
                interrupted()
                schedDebug("[$name] park()")
                park()
            }
        }
    }

    private val numProcessors = Runtime.getRuntime().availableProcessors()
    val minThreadsGoal = max(corePoolSize, min(maxPoolSize, numProcessors))
    val maxThreadsGoal = maxPoolSize

    private val workQueue = CsBasedWorkQueue(this)
    private val numRequestedWorkers = atomic(0)
    private val counts = ThreadCounts(minThreadsGoal)

    private var currentSampleStartTime = 0L
    private var threadAdjustmentIntervalMs = 0
    private var completionCount = 0
    private var priorCompletionCount = 0
    private val hillClimber = HillClimbing(minThreadsGoal, maxThreadsGoal)
    private var nextCompletedWorkRequestsTime = 0L
    private var priorCompletedWorkRequestTime = 0L
    private var nextThreadId = atomic(1)

    private val _isTerminated = atomic(false)
    val isTerminated: Boolean get() = _isTerminated.value
    private val workerStack = Stack<Worker>()

    override fun dispatch(block: Runnable, taskContext: TaskContext, tailDispatch: Boolean) {
        schedDebug("[$schedulerName] dispatch()")
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

    override fun execute(command: Runnable) = dispatch(command)

    override fun close() {
        shutdown(10_000L)
    }

    override fun shutdown(timeout: Long) {
        if (!_isTerminated.compareAndSet(false, true)) return
        val currentWorker = currentWorker()
        while (true) {
            val worker: Worker?
            synchronized(workerStack) {
                worker = if (workerStack.empty()) null else workerStack.pop()
            }
            if (worker == null) break
            if (worker === currentWorker) continue
            while (worker.isAlive) {
                LockSupport.unpark(worker)
                worker.join(timeout)
            }
        }

        while (true) {
            val task = workQueue.dequeue() ?: break
            runSafely(task)
        }
    }

    private fun currentWorker(): Worker? = (Thread.currentThread() as? Worker)?.takeIf { it.scheduler == this }

    private fun tryUnpark(): Boolean {
        synchronized(workerStack) {
            if (workerStack.empty()) return false
            val worker = workerStack.pop()
            worker.inStack.getAndSet(false)
            LockSupport.unpark(worker)
            return true
        }
    }

    fun requestWorker() {
        schedDebug("[$schedulerName] requestWorker()")
        numRequestedWorkers.incrementAndGet()
        maybeAddWorker()
    }

    fun notifyWorkItemComplete(currentTimeMs: Long): Boolean {
        notifyWorkItemProgress(currentTimeMs)
        return !shouldStopProcessingWorkNow()
    }

    private fun notifyWorkItemProgress(currentTimeMs: Long) {
        if (shouldAdjustMaxWorkersActive(currentTimeMs)) {
            adjustMaxWorkersActive()
        }
    }

    private fun adjustMaxWorkersActive() {
        var addWorker = false
        synchronized(schedulerMonitor) {
            if (counts.numProcessingWork > counts.numThreadsGoal) {
                return
            }

            val currentTicks = System.currentTimeMillis()
            val elapsedMs = currentTicks - currentSampleStartTime

            if (elapsedMs >= threadAdjustmentIntervalMs / 2) {
                val numCompletions = completionCount - priorCompletionCount
                val oldNumThreadsGoal = counts.numThreadsGoal
                val updateResult = hillClimber.update(oldNumThreadsGoal, elapsedMs / 1000.0, numCompletions)
                val newNumThreadsGoal = updateResult.first
                threadAdjustmentIntervalMs = updateResult.second

                if (oldNumThreadsGoal != newNumThreadsGoal) {
                    counts.numThreadsGoal = newNumThreadsGoal
                    if (newNumThreadsGoal > oldNumThreadsGoal) {
                        addWorker = true
                    }
                }

                priorCompletionCount = numCompletions
                nextCompletedWorkRequestsTime = currentTicks + threadAdjustmentIntervalMs
                priorCompletedWorkRequestTime = currentTicks
                currentSampleStartTime = currentTicks
            }
        }
        if (addWorker) {
            maybeAddWorker()
        }
    }

    private fun shouldStopProcessingWorkNow(): Boolean {
        synchronized(schedulerMonitor) {
            if (counts.numProcessingWork <= counts.numThreadsGoal) {
                return false
            }

            counts.numProcessingWork--
            return true
        }
    }

    private fun shouldAdjustMaxWorkersActive(currentTimeMs: Long): Boolean {
        synchronized(schedulerMonitor) {
            val priorTime = priorCompletedWorkRequestTime
            val requiredInterval = nextCompletedWorkRequestsTime - priorTime
            val elapsedInterval = currentTimeMs - priorTime

            if (elapsedInterval < requiredInterval) {
                return false
            }

            if (counts.numProcessingWork > counts.numThreadsGoal) {
                return false
            }

            schedDebug("[$schedulerName] shouldAdjustMaxWorkersActive() returns true")
            return true
        }
    }

    private fun maybeAddWorker() {
        schedDebug("[$schedulerName] maybeAddWorker()")
        val toCreate: Int

        synchronized(schedulerMonitor) {
            if (counts.numProcessingWork >= counts.numThreadsGoal) {
                return
            }

            val newNumProcessingWork = counts.numProcessingWork + 1
            val newNumExistingThreads = max(counts.numExistingThreads, newNumProcessingWork)
            toCreate = newNumExistingThreads - counts.numExistingThreads
            counts.numExistingThreads = newNumExistingThreads
            counts.numProcessingWork = newNumProcessingWork
        }

        repeat(toCreate) {
            if (tryUnpark()) return
            createWorker()
        }
    }

    private fun createWorker() {
        val worker = Worker()
        worker.start()
    }

    private fun removeWorkingWorker() {
        synchronized(schedulerMonitor) {
            counts.numProcessingWork--
        }

        if (numRequestedWorkers.value > 0) {
            maybeAddWorker()
        }
    }

    private fun takeActiveRequest(): Boolean {
        var cnt = numRequestedWorkers.value
        while (cnt > 0) {
            if (numRequestedWorkers.compareAndSet(cnt, cnt - 1)) {
                return true
            }
            cnt = numRequestedWorkers.value
        }
        return false
    }
}