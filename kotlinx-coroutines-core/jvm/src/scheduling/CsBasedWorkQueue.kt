/*
 * Copyright 2016-2023 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.scheduling

import kotlin.random.*
import java.util.concurrent.*
import kotlinx.atomicfu.*
import kotlinx.coroutines.*
import kotlinx.coroutines.internal.*

internal class CsBasedWorkQueue(private val scheduler: CsBasedCoroutineScheduler) {
    companion object {
        private const val DISPATCH_QUANTUM_MS = 30L
    }

    private val workItems = ConcurrentLinkedQueue<Task>()
    private var hasOutstandingThreadRequest = atomic(0)

    fun enqueue(task: Task, forceGlobal: Boolean) {
        // TODO - remove, added to compile
        require(forceGlobal)
        workItems.add(task)
        ensureThreadRequested()
    }

    fun dispatch(): Boolean {
        markThreadRequestSatisifed()

        var workItem: Task? = dequeue() ?: return true

        // TODO - check if it is required here.
        // for now removed, creates excess threads
//        ensureThreadRequested()

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

    private fun ensureThreadRequested() {
        // TODO - check if other fix possible
        // Scenario not working for now:
        // 100 blocking tasks launched, since contention some fail this if and ~30 get created, since
        // blocking adjustment adds at most 1 thread at a time
        // possible other fix - add more threads at a time during blocking adjustment
//        if (hasOutstandingThreadRequest.compareAndSet(0, 1)) {
            scheduler.requestWorker()
//        }
    }

    fun dequeue(): Task? {
        return workItems.poll()
    }

    private fun markThreadRequestSatisifed() {
        hasOutstandingThreadRequest.getAndSet(0)
    }

    private fun dispatchWorkItem(workItem: Task) {
//        scheduler.beforeTask(workItem.mode)
        runSafely(workItem)
        scheduler.afterTask(workItem.mode)
    }
}
