/*
 * Copyright 2016-2023 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.scheduling

import kotlinx.atomicfu.*
import java.util.concurrent.*

// [TODO] Take care of overflows and signs.
// [TODO] Check for optimal spinCount.

@Suppress("NOTHING_TO_INLINE")
internal class LowLevelLifoSemaphore(
    private val initialSignalCount: Int,
    private val spinCount: Int
) {
    val data = atomic(0L)

    init {
        require(initialSignalCount >= 0)
        require(spinCount >= 0)

        data.value = (data.value.setSignalCount(initialSignalCount))
    }

    companion object {
        private const val SPIN_YIELD_THRESHOLD = 10
        private const val SPIN_WAITS_PER_ITERATION = 1024

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

    // ===== Helper functions for state managing =====

    private inline fun Long.getValue(mask: Long, shift: Int) =
        ((this and mask) shr shift).toInt()

    private inline fun Long.setValue(value: Int, mask: Long, shift: Int) =
        (this and mask.inv()) or (value.toLong() shl shift)

    private val Long.signalCount
        inline get() = getValue(SIGNAL_COUNT_MASK, SIGNAL_COUNT_SHIFT)
    private val Long.waiterCount
        inline get() = getValue(WAITER_COUNT_MASK, WAITER_COUNT_SHIFT)
    private val Long.spinnerCount
        inline get() = getValue(SPINNER_COUNT_MASK, SPINNER_COUNT_SHIFT)
    private val Long.signaledToWakeCount
        inline get() = getValue(SIGNALED_TO_WAKE_COUNT_MASK, SIGNALED_TO_WAKE_COUNT_SHIFT)

    private inline fun Long.setSignalCount(value: Int) =
        setValue(value, SIGNAL_COUNT_MASK, SIGNAL_COUNT_SHIFT)
    private inline fun Long.setWaiterCount(value: Int) =
        setValue(value, WAITER_COUNT_MASK, WAITER_COUNT_SHIFT)
    private inline fun Long.setSpinnerCount(value: Int) =
        setValue(value, SPINNER_COUNT_MASK, SPINNER_COUNT_SHIFT)
    private inline fun Long.setSignaledToWakeCount(value: Int) =
        setValue(value, SIGNALED_TO_WAKE_COUNT_MASK, SIGNALED_TO_WAKE_COUNT_SHIFT)

    private inline fun Long.addSignalCount(value: Int) =
        this + (value.toLong() shl SIGNAL_COUNT_SHIFT)
    private inline fun Long.incrementSignalCount() =
        this + (1L shl SIGNAL_COUNT_SHIFT)
    private inline fun Long.decrementSignalCount() =
        this - (1L shl SIGNAL_COUNT_SHIFT)
    private inline fun Long.incrementWaiterCount() =
        this + (1L shl WAITER_COUNT_SHIFT)
    private inline fun Long.decrementWaiterCount() =
        this - (1L shl WAITER_COUNT_SHIFT)
    private inline fun Long.incrementSpinnerCount() =
        this + (1L shl SPINNER_COUNT_SHIFT)
    private inline fun Long.decrementSpinnerCount() =
        this - (1L shl SPINNER_COUNT_SHIFT)
    private inline fun Long.decrementSignaledToWakeCount() =
        this - (1L shl SIGNALED_TO_WAKE_COUNT_SHIFT)

    private inline fun Long.addUpToMaxSignaledToWakeCount(value: Int): Long {
        val availableCount = 255 - signaledToWakeCount
        val toAdd = minOf(availableCount, value).toLong()
        return this + (toAdd shl SIGNALED_TO_WAKE_COUNT_SHIFT)
    }

    private inline fun interlockedDecrementWaiterCount() =
        data.getAndAdd(-1L shl WAITER_COUNT_SHIFT)

    // ===============================================

    var counter = 0

    private val semaphore = Semaphore(0)

    private fun spinWait(spinIndex: Int) {
        if (spinIndex < SPIN_YIELD_THRESHOLD || (spinIndex - SPIN_YIELD_THRESHOLD) % 2 != 0) {
            var n = SPIN_WAITS_PER_ITERATION

            if (spinIndex <= 30 && (1 shl spinIndex) < SPIN_WAITS_PER_ITERATION) {
                n = 1 shl spinIndex
            }

            repeat(n) {
                counter++
            }
        } else {
            Thread.yield()
        }
    }

    fun release(releaseCount: Int) {
        require(releaseCount > 0)

        while (true) {
            val counts = data.value
            val newCounts = counts.addSignalCount(releaseCount)

            // Determine how many waiters to wake, taking into account how many spinners and waiters there are and how many waiters
            // have previously been signaled to wake but have not yet woken
            var countOfWaitersToWake =
                minOf(newCounts.signalCount, counts.waiterCount + counts.spinnerCount) -
                    counts.spinnerCount -
                    counts.signaledToWakeCount

            if (countOfWaitersToWake > 0) {
                // Ideally, limiting to a maximum of releaseCount would not be necessary and could be an assert instead, but since
                // WaitForSignal() does not have enough information to tell whether a woken thread was signaled, and due to the cap
                // below, it's possible for countOfWaitersSignaledToWake to be less than the number of threads that have actually
                // been signaled to wake.
                if (countOfWaitersToWake > releaseCount) {
                    countOfWaitersToWake = releaseCount
                }

                // Cap countOfWaitersSignaledToWake to its max value. It's ok to ignore some woken threads in this count, it just
                // means some more threads will be woken next time. Typically, it won't reach the max anyway.
                newCounts.addUpToMaxSignaledToWakeCount(countOfWaitersToWake)
            }

            if (data.compareAndSet(counts, newCounts)) {
                if (countOfWaitersToWake > 0) {
                    releaseCore(countOfWaitersToWake)
                }

                return
            }
        }
    }

    fun wait(timeoutMs: Long, spinWait: Boolean): Boolean {
        require(timeoutMs >= -1)

        val spins = if (spinWait) spinCount else 0

        // Try to acquire the semaphore or
        // a) register as a spinner if spinCount > 0 and timeoutMs > 0
        // b) register as a waiter if there's already too many spinners or spinCount == 0 and timeoutMs > 0
        // c) bail out if timeoutMs == 0 and return false
        while (true) {
            val counts = data.value
            var newCounts = counts

            if (counts.signalCount != 0) {
                newCounts = newCounts.decrementSignalCount()
            } else if (timeoutMs != 0L) {
                if (spins > 0 && newCounts.spinnerCount < MAX_SPINNER_COUNT) {
                    newCounts = newCounts.incrementSpinnerCount()
                } else {
                    // Maximum number of spinners reached or requested not to spin, register as a waiter instead.
                    newCounts = newCounts.incrementWaiterCount()
                }
            }

            if (data.compareAndSet(counts, newCounts)) {
//                System.err.println("Finished wait: \n" +
//                    " > On enter: (${counts.signalCount}, ${counts.spinnerCount}, ${counts.waiterCount}, ${counts.signaledToWakeCount})\n" +
//                    " > Updated: (${newCounts.signalCount}, ${newCounts.spinnerCount}, ${newCounts.waiterCount}, ${newCounts.signaledToWakeCount})")

                if (counts.signalCount != 0) {
                    // Acquired semaphore successfully.
                    return true
                }
                if (newCounts.waiterCount != counts.waiterCount) {
                    // Registered as a waiter.
                    return waitForSignal(timeoutMs)
                }
                if (timeoutMs == 0L) {
                    // Unsuccessful acquire, bail out.
                    return false
                }

                // Registered as a spinner
                break
            }
        }

        var spinIndex = 0
        while (spinIndex < spinCount) {
            spinWait(spinIndex)
            spinIndex++

            while (true) {
                // Try to acquire the semaphore and unregister as a spinner.
                val counts = data.value

                if (counts.signalCount <= 0) {
                    break
                }

                var newCounts = counts

                newCounts = newCounts.decrementSignalCount()
                newCounts = newCounts.decrementSpinnerCount()

                if (data.compareAndSet(counts, newCounts)) {
                    return true
                }
            }
        }

        while (true) {
            // Unregister as spinner, and acquire the semaphore or register as a waiter
            val counts = data.value
            var newCounts = counts.decrementSpinnerCount()

            if (counts.signalCount != 0) {
                // Acquired semaphore successfully.
                newCounts = newCounts.decrementSignalCount()
            } else {
                // Cannot acquire, register as a waiter.
                newCounts = newCounts.incrementWaiterCount()
            }

            if (data.compareAndSet(counts, newCounts)) {
                // Return if acquired successfully or waitForSignal
                return counts.signalCount != 0 || waitForSignal(timeoutMs)
            }
        }
    }

    private fun waitForSignal(pTimeoutMs: Long): Boolean {
        var timeoutMs = pTimeoutMs
        require(timeoutMs > 0L || timeoutMs == -1L)

        while (true) {
            val startWaitMs = if (timeoutMs != -1L) System.currentTimeMillis() else 0
            if (timeoutMs == 0L || !waitCore(timeoutMs)) {
                // Timed out. Unregister the waiter.
                interlockedDecrementWaiterCount()
                return false
            }
            val endWaitMs = if (timeoutMs != -1L) System.currentTimeMillis() else 0

            while (true) {
                // Unregister the waiter if this thread will not be waiting anymore, and try to acquire the semaphore
                val counts = data.value
                require(counts.waiterCount != 0)

                var newCounts = counts

                if (counts.signalCount != 0) {
                    newCounts = newCounts.decrementSignalCount()
                    newCounts = newCounts.decrementWaiterCount()
                }

                if (counts.signaledToWakeCount != 0) {
                    // This waiter has woken up and this needs to be reflected in the count of waiters signaled to wake
                    newCounts = newCounts.decrementSignaledToWakeCount()
                }

                if (data.compareAndSet(counts, newCounts)) {
                    if (counts.signalCount != 0) {
                        // Successfully acquired semaphore.
                        return true
                    }
                    break
                }

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

    private fun waitCore(timeoutMs: Long): Boolean {
        return semaphore.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS)
    }

    private fun releaseCore(count: Int) {
        semaphore.release(count)
    }
}
