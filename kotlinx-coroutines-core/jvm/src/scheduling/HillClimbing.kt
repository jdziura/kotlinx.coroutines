/*
 * Copyright 2016-2023 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.scheduling

import kotlin.random.Random
import kotlin.math.*

internal class HillClimbing {
    private class Complex(val re: Double, val im: Double) {
        operator fun plus(other: Complex): Complex {
            return Complex(re + other.re, im + other.im)
        }

        operator fun minus(other: Complex): Complex {
            return Complex(re - other.re, im - other.im)
        }

        operator fun times(k: Double): Complex {
            return Complex(k * re, k * im)
        }

        operator fun times(other: Complex): Complex {
            return Complex(re * other.re - im * other.im, re * other.im + im * other.re)
        }

        operator fun div(k: Double): Complex {
            return Complex(re / k, im / k)
        }

        operator fun div(other: Complex): Complex {
            return Complex(re * other.re + im * other.im, im * other.re - re * other.im) / other.norm()
        }

        private fun norm(): Double {
            return re * re + im * im
        }

        fun abs(): Double {
            return sqrt(re * re + im * im)
        }
    }

    enum class StateOrTransition {
        Warmup,
        Initializing,
        RandomMove,
        ClimbingMove,
        ChangePoint,
        Stabilizing,
        Starvation,
        ThreadTimedOut,
        CooperativeBlocking
    }
    
    companion object {
        private const val WAVE_PERIOD = 4
        private const val MAX_THREAD_WAVE_MAGNITUDE = 20
        private const val THREAD_MAGNITUDE_MULTIPLIER = 100.0 / 100.0
        private const val SAMPLES_TO_MEASURE = WAVE_PERIOD * 8
        private const val TARGET_THROUGHPUT_RATIO = 15.0 / 100.0
        private const val TARGET_SIGNAL_TO_NOISE_RATIO = 300.0 / 100.0
        private const val MAX_CHANGE_PER_SECOND = 4
        private const val MAX_CHANGE_PER_SAMPLE = 20
        private const val SAMPLE_INTERVAL_MS_LOW = 10
        private const val SAMPLE_INTERVAL_MS_HIGH = 200
        private const val THROUGHPUT_ERROR_SMOOTHING_FACTOR = 1.0 / 100.0
        private const val GAIN_EXPONENT = 200.0 / 100.0
        private const val MAX_SAMPLE_ERROR = 15.0 / 100.0
    }

    private val samples = DoubleArray(SAMPLES_TO_MEASURE)
    private val threadCounts = DoubleArray(SAMPLES_TO_MEASURE)
    private var currentSampleMs = Random.nextInt(SAMPLE_INTERVAL_MS_LOW, SAMPLE_INTERVAL_MS_HIGH + 1)
    private var lastThreadCount = 0
    private var accumulatedSampleDurationSeconds = 0.0
    private var accumulatedCompletionCount = 0
    private var totalSamples = 0L
    private var averageThroughputNoise = 0.0
    private var currentControlSetting = 0.0

    fun update(currentThreadCount: Int, pSampleDurationSeconds: Double, pNumCompletions: Int): Pair<Int, Int> {
        if (currentThreadCount != lastThreadCount) {
            forceChange(currentThreadCount, StateOrTransition.Initializing)
        }

        val sampleDurationSeconds = pSampleDurationSeconds + accumulatedSampleDurationSeconds
        val numCompletions = pNumCompletions + accumulatedCompletionCount

        if (totalSamples > 0 && ((currentThreadCount - 1.0) / numCompletions) >= MAX_SAMPLE_ERROR) {
            accumulatedSampleDurationSeconds = sampleDurationSeconds
            accumulatedCompletionCount = numCompletions
            return (currentThreadCount to 10)
        }

        accumulatedSampleDurationSeconds = 0.0
        accumulatedCompletionCount = 0

        val throughput = numCompletions / sampleDurationSeconds

        val sampleIndex = (totalSamples % SAMPLES_TO_MEASURE).toInt()
        samples[sampleIndex] = throughput
        threadCounts[sampleIndex] = currentThreadCount.toDouble()
        totalSamples++

        var ratio = Complex(0.0, 0.0)
        var confidence = 0.0
        var state = StateOrTransition.Warmup

        val sampleCount = (min(totalSamples - 1, SAMPLES_TO_MEASURE.toLong()).toInt() / WAVE_PERIOD) * WAVE_PERIOD

        if (sampleCount > WAVE_PERIOD) {
            var sampleSum = 0.0
            var threadSum = 0.0

            for (i in 0 until sampleCount) {
                sampleSum += samples[((totalSamples - sampleCount + i) % SAMPLES_TO_MEASURE).toInt()]
                threadSum += threadCounts[((totalSamples - sampleCount + i) % SAMPLES_TO_MEASURE).toInt()]
            }

            val averageThroughput = sampleSum / sampleCount
            val averageThreadCount = threadSum / sampleCount

            if (averageThroughput > 0 && averageThreadCount > 0) {
                val adjacentPeriod1 = sampleCount / ((sampleCount.toDouble() / WAVE_PERIOD) + 1)
                val adjacentPeriod2 = sampleCount / ((sampleCount.toDouble() / WAVE_PERIOD) - 1)

                val throughputWaveComponent = getWaveComponent(samples, sampleCount, WAVE_PERIOD.toDouble()) / averageThroughput
                var throughputErrorEstimate = (getWaveComponent(samples, sampleCount, adjacentPeriod1) / averageThroughput).abs()

                if (adjacentPeriod2 <= sampleCount) {
                    throughputErrorEstimate = max(throughputErrorEstimate, (getWaveComponent(samples, sampleCount, adjacentPeriod2) / averageThroughput).abs())
                }

                val threadWaveComponent = getWaveComponent(threadCounts, sampleCount, WAVE_PERIOD.toDouble()) / averageThreadCount

                averageThroughputNoise = if (averageThroughputNoise == 0.0) {
                    throughputErrorEstimate
                } else {
                    (THROUGHPUT_ERROR_SMOOTHING_FACTOR * throughputErrorEstimate) + ((1.0 - THROUGHPUT_ERROR_SMOOTHING_FACTOR) * averageThroughputNoise)
                }

                // TODO - check if > 0 or > eps
                if (threadWaveComponent.abs() > 0.0) {
                    ratio = (throughputWaveComponent - (threadWaveComponent * TARGET_THROUGHPUT_RATIO)) / threadWaveComponent
                    state = StateOrTransition.ClimbingMove
                } else {
                    ratio = Complex(0.0, 0.0)
                    state = StateOrTransition.Stabilizing
                }

                val noiseForConfidence = max(averageThroughputNoise, throughputErrorEstimate)

                confidence = if (noiseForConfidence > 0.0) {
                    (threadWaveComponent.abs() / noiseForConfidence) / TARGET_SIGNAL_TO_NOISE_RATIO
                } else {
                    1.0
                }
            }
        }

        var move = min(1.0, max(-1.0, ratio.re))
        move *= min(1.0, max(0.0, confidence))

        val gain = MAX_CHANGE_PER_SECOND * sampleDurationSeconds
        move = abs(move).pow(GAIN_EXPONENT) * (if (move >= 0.0) 1.0 else -1.0) * gain
        move = min(move, MAX_CHANGE_PER_SAMPLE.toDouble())

        // TODO: if move > 0 and there is high cpu utilization, don't make a move

        currentControlSetting += move

        var newThreadWaveMagnitude = (0.5 + (currentControlSetting * averageThroughputNoise * TARGET_SIGNAL_TO_NOISE_RATIO * THREAD_MAGNITUDE_MULTIPLIER * 2.0)).toInt()
        newThreadWaveMagnitude = min(newThreadWaveMagnitude, MAX_THREAD_WAVE_MAGNITUDE)
        newThreadWaveMagnitude = max(newThreadWaveMagnitude, 1)

        // TODO - get max/min threads from scheduler
        val maxThreads = CsBasedCoroutineScheduler.MAX_THREADS_GOAL
        val minThreads = CsBasedCoroutineScheduler.MIN_THREADS_GOAL

        currentControlSetting = min(currentControlSetting, (maxThreads - newThreadWaveMagnitude).toDouble())
        currentControlSetting = max(currentControlSetting, minThreads.toDouble())

        var newThreadCount = (currentControlSetting + newThreadWaveMagnitude * ((totalSamples / (WAVE_PERIOD / 2)) % 2)).toInt()

        newThreadCount = min(maxThreads, newThreadCount)
        newThreadCount = max(minThreads, newThreadCount)

//        System.err.println(
//                    "newThreadCount: $newThreadCount\n" +
//                    "currentControlSetting: $currentControlSetting\n" +
//                    "newThreadWaveMagnitude: $newThreadWaveMagnitude\n" +
//                    "totalSamples: $totalSamples\n")

        if (newThreadCount != currentThreadCount) {
            changeThreadCount(newThreadCount, state)
        }

        val newSampleInterval = if (ratio.re < 0.0 && newThreadCount == minThreads) {
            (0.5 + currentSampleMs * (10.0 * min(-ratio.re, 1.0))).toInt()
        } else {
            currentSampleMs
        }

        return (newThreadCount to newSampleInterval)
    }

    fun forceChange(newThreadCount: Int, state: StateOrTransition) {
        if (lastThreadCount != newThreadCount) {
            currentControlSetting += newThreadCount - lastThreadCount
            changeThreadCount(newThreadCount, state)
        }
    }

    private fun changeThreadCount(newThreadCount: Int, state: StateOrTransition) {
        lastThreadCount = newThreadCount
        if (state != StateOrTransition.CooperativeBlocking) {
            currentSampleMs = Random.nextInt(SAMPLE_INTERVAL_MS_LOW, SAMPLE_INTERVAL_MS_HIGH + 1)
        }
    }

    private fun getWaveComponent(samples: DoubleArray, numSamples: Int, period: Double): Complex {
        require(numSamples >= period)
        require(period >= 2)
        require(numSamples <= samples.size)

        val w = 2.0 * PI / period
        val cosine = cos(w)
        val sine = sin(w)
        val coeff = 2.0 * cosine
        var q0: Double
        var q1 = 0.0
        var q2 = 0.0

        for (i in 0 until numSamples) {
            q0 = coeff * q1 - q2 + samples[((totalSamples - numSamples + i) % SAMPLES_TO_MEASURE).toInt()]
            q2 = q1
            q1 = q0
        }

        return Complex(q1 - q2 * cosine, q2 * sine) / numSamples.toDouble()
    }
}
