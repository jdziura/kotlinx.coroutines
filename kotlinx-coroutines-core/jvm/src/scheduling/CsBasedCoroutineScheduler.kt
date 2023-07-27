/*
 * Copyright 2016-2023 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.scheduling

import kotlinx.atomicfu.*
import kotlinx.coroutines.internal.*
import kotlinx.coroutines.trackTask
import java.io.*
import java.lang.Runnable
import java.util.*
import java.util.concurrent.*
import java.util.concurrent.atomic.*
import java.util.concurrent.locks.LockSupport.*
import kotlin.random.*
import kotlin.math.*

internal val schedulerMonitor = Object()

internal const val SCHED_DEBUG = false
internal fun sched_debug(msg: String) {
    if (SCHED_DEBUG)
        System.err.println(msg)
}

internal class CsBasedCoroutineScheduler(
    @JvmField val schedulerName: String = DEFAULT_SCHEDULER_NAME
) : Scheduler {

    companion object {
        const val MIN_THREADS_GOAL = 64
        const val MAX_THREADS_GOAL = 128
    }

    internal inner class Worker : Thread() {
        init {
            isDaemon = true
            name = "$schedulerName-worker-${nextThreadId.getAndIncrement()}"
        }

        inline val scheduler get() = this@CsBasedCoroutineScheduler

        override fun run() = runWorker()

        private fun runWorker() {
            sched_debug("[WORKER] created")
//            System.err.println("Active workers: ${activeWorkers.incrementAndGet()}")
            while (true) {
                doWork()
                if (shouldExitWorker()) {
                    sched_debug("[WORKER] exiting")
//                    System.err.println("Active workers: ${activeWorkers.decrementAndGet()}")
                    break
                }
            }
        }

        private fun doWork() {
            sched_debug("[WORKER] doWork()")
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
                counts.numThreadsGoal = max(1, min(counts.numExistingThreads, counts.numThreadsGoal))

                hillClimber.forceChange(counts.numThreadsGoal, HillClimbing.StateOrTransition.ThreadTimedOut)
                return true
            }
        }
    }

    private val workQueue = CsBasedWorkQueue(this)
    private val numRequestedWorkers = atomic(0)
    private val counts = ThreadCounts()

    private var currentSampleStartTime = 0L
    private var threadAdjustmentIntervalMs = 0
    private var completionCount = 0
    private var priorCompletionCount = 0
    private val hillClimber = HillClimbing()
    private var nextCompletedWorkRequestsTime = 0L
    private var priorCompletedWorkRequestTime = 0L
    private var nextThreadId = atomic(1)

    private var activeWorkers = atomic(0)

    override fun dispatch(block: Runnable, taskContext: TaskContext, tailDispatch: Boolean) {
        sched_debug("[SCHEDULER] dispatch()")
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

    }

    fun requestWorker() {
        sched_debug("[SCHEDULER] requestWorker()")
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
        synchronized(schedulerMonitor) {
            var addWorker = false

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

            if (addWorker) {
                maybeAddWorker()
            }
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
        synchronized(this) {
            val priorTime = priorCompletedWorkRequestTime
            val requiredInterval = nextCompletedWorkRequestsTime - priorTime
            val elapsedInterval = currentTimeMs - priorTime

            if (elapsedInterval < requiredInterval) {
                return false
            }

            if (counts.numProcessingWork > counts.numThreadsGoal) {
                return false
            }

            sched_debug("[SCHEDULER] shouldAdjustMaxWorkersActive() returns true")
            return true
        }
    }

    private fun maybeAddWorker() {
        sched_debug("[SCHEDULER] maybeAddWorker()")
        val toCreate: Int

        synchronized(schedulerMonitor) {
            if (counts.numProcessingWork >= counts.numThreadsGoal) {
                return
            }

            counts.numProcessingWork++
            val newNumExistingThreads = max(counts.numExistingThreads, counts.numProcessingWork)
            toCreate = newNumExistingThreads - counts.numExistingThreads
            counts.numExistingThreads = newNumExistingThreads
        }

        repeat(toCreate) {
            createWorker()
        }
    }

    private fun createWorker() {
        val worker = Worker()
        worker.start()
    }

    private fun removeWorkingWorker() {
        var oldCounts = counts.copy()
        while (true) {
            val newCounts = oldCounts.copy()
            newCounts.numProcessingWork--

            if (counts.compareAndSet(oldCounts, newCounts)) {
                break
            }

            oldCounts = counts.copy()
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