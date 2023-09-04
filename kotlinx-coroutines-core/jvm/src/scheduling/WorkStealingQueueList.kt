/*
 * Copyright 2016-2021 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.scheduling

// [TODO] Decide if use this instead of workers array. For now not.
internal class WorkStealingQueueList {
    var queues = emptyArray<WorkStealingQueue?>()

    fun add(queue: WorkStealingQueue) {
        synchronized(this) {
            val oldQueues = queues
            require(!oldQueues.contains(queue))
            val newQueues = arrayOfNulls<WorkStealingQueue>(oldQueues.size + 1)
            oldQueues.copyInto(newQueues)
            newQueues[newQueues.lastIndex] = queue
            queues = newQueues
        }
    }

    fun remove(queue: WorkStealingQueue) {
        synchronized(this) {
            val oldQueues = queues
            if (oldQueues.isEmpty()) return

            val newQueues = oldQueues.filter { it != queue }
                .toTypedArray()

            require(newQueues.size == oldQueues.size - 1)
            queues = newQueues
        }
    }
}