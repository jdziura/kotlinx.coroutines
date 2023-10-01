/*
 * Copyright 2016-2023 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.scheduling

import kotlinx.atomicfu.*
import kotlinx.coroutines.internal.*
import kotlinx.coroutines.internal.AVAILABLE_PROCESSORS
import kotlin.math.*
import kotlin.synchronized

internal class DispatchSampler {
    companion object {
        private const val SAMPLE_SIZE = 64        // must be a power of 2
        private const val MASK = SAMPLE_SIZE - 1

        private val MIN_WAITING_TIME: Long = WORK_STEALING_TIME_RESOLUTION_NS / 10
        private val MAX_WAITING_TIME: Long = WORK_STEALING_TIME_RESOLUTION_NS * 100
    }

    private var prevTime: Long = schedulerTimeSource.nanoTime()
    private var lastTime: Long = prevTime

    private val idx = atomic(0)

    @Volatile private var _averageDispatchTime: Long = WORK_STEALING_TIME_RESOLUTION_NS
    val averageDispatchTime: Long inline get() = _averageDispatchTime

    fun notifyDispatch() {
        val currentIdx = idx.incrementAndGet()
        if (currentIdx and MASK == 0) {
            val currentTime = schedulerTimeSource.nanoTime()

            synchronized(this) {
                prevTime = lastTime
                lastTime = currentTime

                val newTime = (lastTime - prevTime) * AVAILABLE_PROCESSORS / SAMPLE_SIZE

                _averageDispatchTime = max(MIN_WAITING_TIME, min(MAX_WAITING_TIME, newTime))
            }
        }
    }
}