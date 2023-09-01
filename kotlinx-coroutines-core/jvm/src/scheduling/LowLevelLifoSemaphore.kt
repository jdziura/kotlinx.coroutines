/*
 * Copyright 2016-2023 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.scheduling

import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicLong
import kotlin.random.Random

// [TODO] Take care of overflows and sign
// [TODO] Check for optimal spinCount (maybe *2 like in C# CORECLR && UNIX)

@Suppress("NOTHING_TO_INLINE")
internal abstract class LowLevelLifoSemaphoreBase(
    protected val initialSignalCount: Int,
    protected val maximumSignalCount: Int,
    protected val spinCount: Int
) {
    val data = AtomicLong(0L)

    init {
        require(initialSignalCount >= 0)
        require(initialSignalCount <= maximumSignalCount)
        require(maximumSignalCount > 0)
        require(spinCount >= 0)

        data.set(data.get().setSignalCount(initialSignalCount))
    }

    // ===== Helper functions for state managing =====

    companion object {
        private const val SIGNAL_COUNT_SHIFT = 0
        private const val WAITER_COUNT_SHIFT = 32
        private const val SPINNER_COUNT_SHIFT = 48
        private const val SIGNALED_TO_WAKE_COUNT_SHIFT = 56

        private const val SIGNAL_COUNT_MASK = (1L shl 32) - 1
        private const val WAITER_COUNT_MASK = ((1L shl 16) - 1) shl WAITER_COUNT_SHIFT
        private const val SPINNER_COUNT_MASK = ((1L shl 8) - 1) shl SPINNER_COUNT_SHIFT
        private const val SIGNALED_TO_WAKE_COUNT_MASK = ((1L shl 8) - 1) shl SIGNALED_TO_WAKE_COUNT_SHIFT

        const val MAX_SPINNER_COUNT = 255
    }

    inline fun Long.getValue(mask: Long, shift: Int) =
        ((this and mask) shr shift).toInt()

    inline fun Long.setValue(value: Int, mask: Long, shift: Int) =
        (this and mask.inv()) or (value.toLong() shl shift)

    val Long.signalCount
        inline get() = getValue(SIGNAL_COUNT_MASK, SIGNAL_COUNT_SHIFT)
    val Long.waiterCount
        inline get() = getValue(WAITER_COUNT_MASK, WAITER_COUNT_SHIFT)
    val Long.spinnerCount
        inline get() = getValue(SPINNER_COUNT_MASK, SPINNER_COUNT_SHIFT)
    val Long.signaledToWakeCount
        inline get() = getValue(SIGNALED_TO_WAKE_COUNT_MASK, SIGNALED_TO_WAKE_COUNT_SHIFT)

    inline fun Long.setSignalCount(value: Int) =
        setValue(value, SIGNAL_COUNT_MASK, SIGNAL_COUNT_SHIFT)
    inline fun Long.setWaiterCount(value: Int) =
        setValue(value, WAITER_COUNT_MASK, WAITER_COUNT_SHIFT)
    inline fun Long.setSpinnerCount(value: Int) =
        setValue(value, SPINNER_COUNT_MASK, SPINNER_COUNT_SHIFT)
    inline fun Long.setSignaledToWakeCount(value: Int) =
        setValue(value, SIGNALED_TO_WAKE_COUNT_MASK, SIGNALED_TO_WAKE_COUNT_SHIFT)

    inline fun Long.addSignalCount(value: Int) =
        this + (value.toLong() shl SIGNAL_COUNT_SHIFT)
    inline fun Long.incrementSignalCount() =
        this + (1L shl SIGNAL_COUNT_SHIFT)
    inline fun Long.decrementSignalCount() =
        this - (1L shl SIGNAL_COUNT_SHIFT)
    inline fun Long.incrementWaiterCount() =
        this + (1L shl WAITER_COUNT_SHIFT)
    inline fun Long.decrementWaiterCount() =
        this - (1L shl WAITER_COUNT_SHIFT)
    inline fun Long.incrementSpinnerCount() =
        this + (1L shl SPINNER_COUNT_SHIFT)
    inline fun Long.decrementSpinnerCount() =
        this - (1L shl SPINNER_COUNT_SHIFT)
    inline fun Long.decrementSignaledToWakeCount() =
        this - (1L shl SIGNALED_TO_WAKE_COUNT_SHIFT)

    inline fun Long.addUpToMaxSignaledToWakeCount(value: Int): Long {
        val availableCount = 255 - signaledToWakeCount
        val toAdd = minOf(availableCount, value).toLong()
        return this + (toAdd shl SIGNALED_TO_WAKE_COUNT_SHIFT)
    }

    inline fun interlockedDecrementWaiterCount() =
        data.getAndAdd(-1L shl WAITER_COUNT_SHIFT)

    // ===============================================

    fun testCounts() {
        var signalCount = 0
        var waiterCount = 0
        var spinnerCount = 0
        var signaledToWakeCount = 0

        var state = 0L

        val iterations = 100_000_000
        val bound = 100

        for (i in 1..iterations) {
            val value = Random.nextInt(bound)
            when (Random.nextInt(12)) {
                0 -> { // setSignalCount
                    signalCount = value
                    state = state.setSignalCount(value)
                }
                1 -> { // setWaiterCount
                    waiterCount = value
                    state = state.setWaiterCount(value)
                }
                2 -> { // setSpinnerCount
                    spinnerCount = value
                    state = state.setSpinnerCount(value)
                }
                3 -> { // setSignaledToWakeCount
                    signaledToWakeCount = value
                    state = state.setSignaledToWakeCount(value)
                }
                4 -> { // addSignalCount
                    signalCount += value
                    state = state.addSignalCount(value)
                }
                5 -> { // incrementSignalCount
                    signalCount++
                    state = state.incrementSignalCount()
                }
                6 -> { // decrementSignalCount
                    if (signalCount == 0) continue
                    signalCount--
                    state = state.decrementSignalCount()
                }
                7 -> { // incrementWaiterCount
                    waiterCount++
                    state = state.incrementWaiterCount()
                }
                8 -> { // decrementWaiterCount
                    if (waiterCount == 0) continue
                    waiterCount--
                    state = state.decrementWaiterCount()
                }
                9 -> { // incrementSpinnerCount
                    spinnerCount++
                    state = state.incrementSpinnerCount()
                }
                10 -> { // decrementSpinnerCount
                    if (spinnerCount == 0) continue
                    spinnerCount--
                    state = state.decrementSpinnerCount()
                }
                11 -> { // decrementSignaledToWakeCount
                    if (signaledToWakeCount == 0) continue
                    signaledToWakeCount--
                    state = state.decrementSignaledToWakeCount()
                }
            }

            require(signalCount == state.signalCount)
            require(waiterCount == state.waiterCount)
            require(spinnerCount == state.spinnerCount)
            require(signaledToWakeCount == state.signaledToWakeCount)
        }
    }

    protected abstract fun releaseCore(count: Int)

    fun release(releaseCount: Int) {
        require(releaseCount > 0)
        require(releaseCount <= maximumSignalCount)

        var counts = data.get()

        while (true) {
            val newCounts = counts.addSignalCount(releaseCount)
            var countOfWaitersToWake =
                minOf(newCounts.signalCount, counts.waiterCount, counts.spinnerCount) -
                    counts.spinnerCount -
                    counts.signaledToWakeCount

            if (countOfWaitersToWake > 0) {
                if (countOfWaitersToWake > releaseCount) {
                    countOfWaitersToWake = releaseCount
                }

                newCounts.addUpToMaxSignaledToWakeCount(countOfWaitersToWake)
            }

            val oldCounts = data.compareAndExchange(counts, newCounts)
            if (oldCounts == counts) {
                require(releaseCount <= maximumSignalCount - counts.signalCount)

                if (countOfWaitersToWake > 0) {
                    releaseCore(countOfWaitersToWake)
                }

                return
            }

            counts = oldCounts
        }
    }
}

internal class LowLevelLifoSemaphore(
    initialSignalCount: Int,
    maximumSignalCount: Int,
    spinCount: Int
) : LowLevelLifoSemaphoreBase(initialSignalCount, maximumSignalCount, spinCount) {

    val semaphore = Semaphore(0)

    fun wait(timeoutMs: Long, spinWait: Boolean): Boolean {
        require(timeoutMs >= -1)

        val spins = if (spinWait) spinCount else 0

        var counts = data.get()

        while (true) {
            require(counts.signalCount <= maximumSignalCount)

            var newCounts = counts
            if (counts.signalCount != 0) {
                newCounts = newCounts.decrementSignalCount()
            } else if (timeoutMs != 0L) {
                if (spins > 0 && newCounts.spinnerCount < MAX_SPINNER_COUNT) {
                    newCounts = newCounts.incrementSpinnerCount()
                } else {
                    newCounts = newCounts.incrementWaiterCount()
                }
            }

            val oldCounts = data.compareAndExchange(counts, newCounts)
            if (oldCounts == counts) {
                if (counts.signalCount != 0) {
                    return true
                }
                if (newCounts.waiterCount != counts.waiterCount) {
                    return waitForSignal(timeoutMs)
                }
                if (timeoutMs == 0L) {
                    return false
                }
                break
            }

            counts = oldCounts
        }

        var spinIndex = 0
        while (spinIndex < spinCount) {
            // [TODO] Check for .NET alternatives for yield() (there is a custom spin waiter)
            Thread.yield()

            spinIndex++
            counts = data.get()
            while (counts.signalCount > 0) {
                var newCounts = counts

                newCounts = newCounts.decrementSignalCount()
                newCounts = newCounts.decrementSpinnerCount()

                val oldCounts = data.compareAndExchange(counts, newCounts)
                if (oldCounts == counts) {
                    return true
                }

                counts = oldCounts
            }
        }

        counts = data.get()
        while (true) {
            var newCounts = counts.decrementSpinnerCount()
            if (counts.signalCount != 0) {
                newCounts = newCounts.decrementSignalCount()
            } else {
                newCounts = newCounts.incrementWaiterCount()
            }

            val oldCounts = data.compareAndExchange(counts, newCounts)
            if (oldCounts == counts) {
                return counts.signalCount != 0 || waitForSignal(timeoutMs)
            }

            counts = oldCounts
        }
    }

    private fun waitCore(timeoutMs: Long): Boolean {
        return semaphore.tryAcquire(1, timeoutMs, TimeUnit.MILLISECONDS)
    }

    override fun releaseCore(count: Int) {
        semaphore.release(count)
    }

    private fun waitForSignal(pTimeoutMs: Long): Boolean {
        var timeoutMs = pTimeoutMs
        require(timeoutMs > 0L || timeoutMs == -1L)

        while (true) {
            val startWaitMs = if (timeoutMs != -1L) System.currentTimeMillis() else 0
            if (timeoutMs == 0L || !waitCore(timeoutMs)) {
                interlockedDecrementWaiterCount()
                return false
            }
            val endWaitMs = if (timeoutMs != -1L) System.currentTimeMillis() else 0

            var counts = data.get()
            while (true) {
                require(counts.waiterCount != 0)
                var newCounts = counts
                if (counts.signalCount != 0) {
                    newCounts = newCounts.decrementSignalCount()
                    newCounts = newCounts.decrementWaiterCount()
                }

                if (counts.signaledToWakeCount != 0) {
                    newCounts = newCounts.decrementSignaledToWakeCount()
                }

                val oldCounts = data.compareAndExchange(counts, newCounts)
                if (oldCounts == counts) {
                    if (counts.signalCount != 0) {
                        return true
                    }
                    break
                }

                counts = oldCounts
                if (timeoutMs != -1L) {
                    val waitMs = endWaitMs - startWaitMs
                    if (waitMs >= 0L && waitMs < timeoutMs) {
                        timeoutMs -= waitMs
                    } else {
                        timeoutMs = 0L
                    }
                }
            }
        }
    }
}
