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
import java.util.concurrent.CyclicBarrier
import kotlin.collections.ArrayDeque
import kotlin.random.*
import kotlin.random.Random

// TODO - do some tests
internal class AutoResetEvent(
    private var open: Boolean
) {
    private val monitor = Object()
    fun waitOne() {
        synchronized(monitor) {
            while (!open) {
                monitor.wait()
            }
            open = false
        }
    }

    fun waitOne(timeout: Long): Boolean {
        synchronized(monitor) {
            val timestamp = System.currentTimeMillis()
            while (!open) {
                monitor.wait(timeout)
                if (System.currentTimeMillis() - timestamp >= timeout) {
                    open = false
                    return false
                }
            }
            open = false
            return true
        }
    }

    fun set() {
        synchronized(monitor) {
            open = true
            monitor.notify()
        }
    }
}

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

    fun requestWorker() {
        numRequestedWorkers.incrementAndGet()
        maybeAddWorker()
        ensureGateThread()
    }

    override fun dispatch(block: Runnable, taskContext: TaskContext, tailDispatch: Boolean) {
        trackTask()
        val task = createTask(block, taskContext)
        System.err.println("dispatch")
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

    var workingThreads = 0
    val maxWorkingThreads = 8
    fun maybeAddWorker() {
        // TODO - implement
        synchronized(this) {
            if (workingThreads < maxWorkingThreads) {
                workingThreads++
                createWorker()
            }
        }
    }

    fun createWorker() {
        synchronized(this) {
            val worker = Worker()
            worker.start()
        }
    }

    // TODO - const
    val maxRuns = 2

    // TODO - const
    val gateThreadRunningMask = 0x4

    val gateThreadRunningState = atomic(0)
    val runGateThreadEvent = AutoResetEvent(true)
    val delayEvent = AutoResetEvent(false)

    fun getRunningStateForNumRuns(numRuns: Int): Int {
        require(numRuns >= 0)
        require(numRuns <= maxRuns)
        return gateThreadRunningMask or numRuns
    }

    fun ensureGateThread() {
        synchronized(this) {
            if (gateThreadRunningState.value == getRunningStateForNumRuns(maxRuns)) {
                return
            }

            val numRunsMask = gateThreadRunningState.getAndSet(getRunningStateForNumRuns(maxRuns))
            if (numRunsMask == getRunningStateForNumRuns(0)) {
                runGateThreadEvent.set()
            } else if ((numRunsMask and gateThreadRunningMask) == 0) {
                createGateThread()
            }
        }
    }

    fun createGateThread() {
        // TODO - C# has dedicated stack size for that thread, investigate
        val gateThread = GateThread()
        gateThread.start()
    }

    var previousGateActivitiesTimeMs = 0L

//    // TODO - check if thread safe
//    var pendingBlockingAdjustment = PendingBlockingAdjustment.None
//
//    // TODO - pack in DelayHelper
//    var previousBlockingAdjustmentDelayMs = 0
//    var previousBlockingAdjustmentDelatStartTimeMs = 0L
//    val hasBlockingAdjustmentDelay: Boolean
//        get() = previousBlockingAdjustmentDelayMs != 0
    fun getNextDelay(currentTimeMs: Long): Long {
        // TODO - implement

        // const delay for now, ugly val to compile without warnings
        val retValue = 500L + currentTimeMs - currentTimeMs
        return retValue
    }
//
//    fun hasBlockingAdjustmentDelayElapsed(currentTimeMs: Long, wasSignaledToWake: Boolean): Boolean {
//        require(hasBlockingAdjustmentDelay)
//        if (!wasSignaledToWake && adjustForBlockingAfterNextDelay) {
//            return true
//        }
//
//        val elapsedMsSincePreviousBlockingAdjustmentDelay =
//            currentTimeMs - previousBlockingAdjustmentDelayStartTimeMs
//
//        return elapsedMsSincePreviousBlockingAdjustmentDelay >= previousBlockingAdjustmentDelayMs
//    }
//
//    enum class PendingBlockingAdjustment {
//        None, Immediately, WithDelayIfNecessary
//    }
//
//    fun performBlockingAdjustment(previousDelayElapsed: Boolean): Long {
//
//    }

    internal inner class GateThread : Thread() {
        init {
            isDaemon = true
        }

        inline val scheduler get() = this@CsBasedCoroutineScheduler

        override fun run() = runGateThread()

        private fun runGateThread() {
            // TODO - initialization in C# GateThreadStart()
            System.err.println("GateThread started")
            while (true) {
                runGateThreadEvent.waitOne()
                var currentTimeMs = System.currentTimeMillis()
                previousGateActivitiesTimeMs = currentTimeMs

                while (true) {
                    val wasSignaledToWake = delayEvent.waitOne(getNextDelay(currentTimeMs))
                    currentTimeMs = System.currentTimeMillis()

                    // TODO - understand what is going on and implement
                    // for now: skip "blockingAdjustment" and everything with it
//                    do {
//                        if (pendingBlockingAdjustment == PendingBlockingAdjustment.None) {
//                            previousBlockingAdjustmentDelayMs = 0
//                            break
//                        }
//
//                        var previousDelayElapsed = false
//                        if (hasBlockingAdjustmentDelay) {
//                            previousDelayElapsed =
//                                hasBlockingAdjustmentDelayElapsed(currentTimeMs, wasSignaledToWake)
//
//                            if (pendingBlockingAdjustment == PendingBlockingAdjustment.WithDelayIfNecessary && !previousDelayElapsed) {
//                                break
//                            }
//                        }
//
//                        val nextDelayMs = performBlockingAdjustment(previousDelayElapsed)
//                    } while (false)
                }
            }
        }
    }

    internal inner class Worker : Thread() {
        init {
            isDaemon = true
        }

        inline val scheduler get() = this@CsBasedCoroutineScheduler

        override fun run() = runWorker()

        private fun runWorker() {
            // TODO - locking
            System.err.println("Worker created!")
        }
    }

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