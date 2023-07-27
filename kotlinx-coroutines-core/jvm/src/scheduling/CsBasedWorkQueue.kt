/*
 * Copyright 2016-2023 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.scheduling

import kotlin.random.*
import java.util.concurrent.*
import kotlinx.atomicfu.*
import kotlinx.coroutines.*
import kotlinx.coroutines.unTrackTask

internal class CsBasedWorkQueue(private val scheduler: CsBasedCoroutineScheduler) {
    companion object {
        private const val DISPATCH_QUANTUM_MS = 30L
    }

    private val workItems = ConcurrentLinkedQueue<Task>()
    private var hasOutstandingThreadRequest = atomic(0)

    private fun ensureThreadRequested() {
        sched_debug("[QUEUE] ensureThreadRequested()")
        if (hasOutstandingThreadRequest.compareAndSet(0, 1)) {
            scheduler.requestWorker()
        }
    }

    fun enqueue(task: Task, forceGlobal: Boolean) {
        sched_debug("[QUEUE] enqueue()")
        // TODO - remove, added to compile
        require(forceGlobal)
        workItems.add(task)
        ensureThreadRequested()
    }

    fun dequeue(): Task? {
        return workItems.poll()
    }

    fun markThreadRequestSatisifed() {
        hasOutstandingThreadRequest.getAndSet(0)
    }

    fun dispatch(): Boolean {
        sched_debug("[QUEUE] dispatch()")
        markThreadRequestSatisifed()

        var workItem: Task? = dequeue() ?: return true

        ensureThreadRequested()

        var startTickCount = System.currentTimeMillis()

        while (true) {
            if (workItem == null) {
                workItem = dequeue()
                if (workItem == null) {
                    return true
                }
            }

            dispatchWorkItem(workItem)
            workItem = null

            val currentTickCount = System.currentTimeMillis()
            if (!scheduler.notifyWorkItemComplete(currentTickCount)) {
                return false
            }

            if (currentTickCount - startTickCount < DISPATCH_QUANTUM_MS) {
                continue
            }

            startTickCount = currentTickCount
        }
    }

    fun dispatchWorkItem(workItem: Task) {
        runSafely(workItem)
    }

    fun runSafely(task: Task) {
        sched_debug("[TASK] >> Running task")
        try {
            task.run()
        } catch (e: Throwable) {
            val thread = Thread.currentThread()
            thread.uncaughtExceptionHandler.uncaughtException(thread, e)
        } finally {
            unTrackTask()
        }
    }
}
