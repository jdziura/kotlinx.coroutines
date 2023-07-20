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
import java.util.concurrent.locks.LockSupport.*
import java.util.concurrent.locks.ReentrantLock
import kotlin.collections.ArrayDeque
import kotlin.random.*
import kotlin.random.Random

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

    override fun dispatch(block: Runnable, taskContext: TaskContext, tailDispatch: Boolean) {
        trackTask()
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

    internal inner class Worker private constructor() : Thread() {
        init {
            isDaemon = true
        }

        var indexInArray = 0
            set(index) {
                name = "$schedulerName-worker-$index"
                field = index
            }

        constructor(index: Int) : this() {
            indexInArray = index
        }
        inline val scheduler get() = this@CsBasedCoroutineScheduler

        override fun run() = runWorker()

        private fun runWorker() {}
    }

    internal inner class HillClimbing private constructor() {
        init {

        }

        fun update(currentThreadCount: Int, sampleDurationSeconds: Double, numCompletions: Int): Int {
            return currentThreadCount
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