/*
 * Copyright 2016-2023 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.scheduling

internal class DelayHelper {
    companion object {
        const val GATE_ACTIVITIES_PERIOD_MS = 10000L
    }
    // TODO - check starting values
    private var previousGateActivitiesTimeMs = 0L
    private var previousBlockingAdjustmentDelayStartTimeMs = 0L
    private var previousBlockingAdjustmentDelayMs = 0L
    private var runGateActivitiesAfterNextDelay = false
    private var adjustForBlockingAfterNextDelay = false

    val hasBlockingAdjustmentDelay: Boolean
        get() = previousBlockingAdjustmentDelayMs != 0L

    fun setGateActivitiesTime(currentTimeMs: Long) {
        previousGateActivitiesTimeMs = currentTimeMs
    }

    fun setBlockingAdjustmentTimeAndDelay(currentTimeMs: Long, delayMs: Long) {
        previousBlockingAdjustmentDelayStartTimeMs = currentTimeMs
        previousBlockingAdjustmentDelayMs = delayMs
    }

    fun clearBlockingAdjustmentDelay() {
        previousBlockingAdjustmentDelayMs = 0L
    }

    fun getNextDelay(currentTimeMs: Long): Long {
        val elapsedMsSincePreviousGateActivities = currentTimeMs - previousGateActivitiesTimeMs
        val nextDelayForGateActivities = if (elapsedMsSincePreviousGateActivities < GATE_ACTIVITIES_PERIOD_MS)
            GATE_ACTIVITIES_PERIOD_MS - elapsedMsSincePreviousGateActivities
        else
            1L

        if (previousBlockingAdjustmentDelayMs == 0L) {
            runGateActivitiesAfterNextDelay = true
            adjustForBlockingAfterNextDelay = false
            return nextDelayForGateActivities
        }

        val elapsedMsSincePreviousBlockingAdjustmentDelay = currentTimeMs - previousBlockingAdjustmentDelayMs
        val nextDelayForBlockingAdjustment = if (elapsedMsSincePreviousBlockingAdjustmentDelay < previousBlockingAdjustmentDelayMs)
            previousBlockingAdjustmentDelayMs - elapsedMsSincePreviousBlockingAdjustmentDelay
        else
            1L

        val nextDelay = minOf(nextDelayForGateActivities, nextDelayForBlockingAdjustment)
        runGateActivitiesAfterNextDelay = nextDelay == nextDelayForGateActivities
        adjustForBlockingAfterNextDelay = nextDelay == nextDelayForBlockingAdjustment

        require(nextDelay <= GATE_ACTIVITIES_PERIOD_MS)

        return nextDelay
    }

    fun shouldPerformGateActivities(currentTimeMs: Long, wasSignedToWake: Boolean): Boolean {
        val result =
            (!wasSignedToWake && runGateActivitiesAfterNextDelay) or
            (currentTimeMs - previousGateActivitiesTimeMs >= GATE_ACTIVITIES_PERIOD_MS)

        if (result) {
            setGateActivitiesTime(currentTimeMs)
        }

        return result
    }

    fun hasBlockingAdjustmentDelayElapsed(currentTimeMs: Long, wasSignaledToWake: Boolean): Boolean {
        require(hasBlockingAdjustmentDelay)
        if (!wasSignaledToWake && adjustForBlockingAfterNextDelay) {
            return true
        }

        val elapsedMsSincePreviousBlockingAdjustmentDelay = currentTimeMs - previousBlockingAdjustmentDelayStartTimeMs
        return elapsedMsSincePreviousBlockingAdjustmentDelay >= previousBlockingAdjustmentDelayMs
    }
}
