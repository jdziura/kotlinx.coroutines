/*
 * Copyright 2016-2023 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.scheduling

import kotlinx.atomicfu.*
import kotlinx.coroutines.internal.*
import kotlinx.coroutines.*
import kotlin.random.Random
import kotlin.math.*
import java.util.*

internal class HillClimbing {
    internal class Complex(val re: Double, val im: Double) {
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

        fun norm(): Double {
            return re * re + im * im
        }

        fun abs(): Double {
            return sqrt(norm())
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
        private const val logCapacity = 200
        private const val wavePeriod = 4
        private const val maxThreadWaveMagnitude = 20
        private const val threadMagnitudeMultiplier = 100
        private const val samplesToMeasure = wavePeriod * 8
        private const val targetThroughputRatio = 15.0 / 100.0
        private const val targetSignalToNoiseRatio = 300.0 / 100.0
        private const val maxChangePerSecond = 4
        private const val maxChangePerSample = 20
        private const val sampleIntervalMsLow = 10
        private const val sampleIntervalMsHigh = 200
        private const val throughputErrorSmoothingFactor = 1.0 / 100.0
        private const val gainExponent = 200.0 / 100.0
        private const val maxSampleError = 15.0 / 100.0
    }

    private val samples = DoubleArray(samplesToMeasure)
    private val threadCounts = DoubleArray(samplesToMeasure)

    private var currentSampleMs = Random.nextInt(sampleIntervalMsLow, sampleIntervalMsHigh + 1)

    private var lastThreadCount = 0
    private var secondsElapsedSinceLastChange = 0.0
    private var completionsSinceLastChange = 0.0
    private var accumulatedSampleDurationSeconds = 0.0
    private var accumulatedCompletionCount = 0
    private var totalSamples = 0L
    private var averageThroughputNoise = 0.0
    private var currentControlSetting = 0.0

    fun update(currentThreadCount: Int, _sampleDurationSeconds: Double, _numCompletions: Int): Pair<Int, Int> {
        if (currentThreadCount != lastThreadCount) {
            forceChange(currentThreadCount, StateOrTransition.Initializing)
        }

        secondsElapsedSinceLastChange += _sampleDurationSeconds
        completionsSinceLastChange += _numCompletions

        val sampleDurationSeconds = _sampleDurationSeconds + accumulatedSampleDurationSeconds
        val numCompletions = _numCompletions + accumulatedCompletionCount

        if (totalSamples > 0 && ((currentThreadCount - 1.0) / numCompletions) >= maxSampleError) {
            accumulatedSampleDurationSeconds = sampleDurationSeconds
            accumulatedCompletionCount = numCompletions
            System.err.println("Early: $totalSamples, $currentThreadCount, $numCompletions")
            return (currentThreadCount to 10)
        }

        accumulatedSampleDurationSeconds = 0.0
        accumulatedCompletionCount = 0

        val throughput = numCompletions / sampleDurationSeconds
//        System.err.println("Throughput measured: $throughput")

        val sampleIndex = (totalSamples % samplesToMeasure).toInt()
        samples[sampleIndex] = throughput
        threadCounts[sampleIndex] = currentThreadCount.toDouble()
        totalSamples++

        var threadWaveComponent: Complex
        var throughputWaveComponent: Complex
        var throughputErrorEstimate: Double
        var ratio = Complex(0.0, 0.0)
        var confidence = 0.0
        var state = StateOrTransition.Warmup

        var sampleCount = min(totalSamples - 1, samplesToMeasure.toLong()).toInt() / wavePeriod * wavePeriod

        if (sampleCount > wavePeriod) {
            var sampleSum = 0.0
            var threadSum = 0.0

            for (i in 0..(sampleCount - 1)) {
                sampleSum += samples[((totalSamples - sampleCount + i) % samplesToMeasure).toInt()]
                threadSum += threadCounts[((totalSamples - sampleCount + i) % samplesToMeasure).toInt()]
            }

            var averageThroughput = sampleSum / sampleCount
            var averageThreadCount = threadSum / sampleCount

            if (averageThroughput > 0 && averageThreadCount > 0) {
                var adjacentPeriod1 = sampleCount / ((sampleCount.toDouble() / wavePeriod) + 1)
                var adjacentPeriod2 = sampleCount / ((sampleCount.toDouble() / wavePeriod) - 1)

                throughputWaveComponent = getWaveComponent(samples, sampleCount, wavePeriod.toDouble()) / averageThroughput
                throughputErrorEstimate = (getWaveComponent(samples, sampleCount, adjacentPeriod1) / averageThroughput).abs()

                if (adjacentPeriod2 <= sampleCount) {
                    throughputErrorEstimate = max(throughputErrorEstimate, (getWaveComponent(samples, sampleCount, adjacentPeriod2) / averageThroughput).abs())
                }

                threadWaveComponent = getWaveComponent(threadCounts, sampleCount, wavePeriod.toDouble()) / averageThreadCount

                if (averageThroughputNoise == 0.0) {
                    averageThroughputNoise = throughputErrorEstimate
                } else {
                    averageThroughputNoise = (throughputErrorSmoothingFactor * throughputErrorEstimate) + ((1.0 - throughputErrorSmoothingFactor) * averageThroughputNoise)
                }

                // TODO - check if > 0 or > eps
                if (threadWaveComponent.abs() > 0.0) {
                    ratio = (throughputWaveComponent - (threadWaveComponent * targetThroughputRatio)) / threadWaveComponent
                    state = StateOrTransition.ClimbingMove
                } else {
                    ratio = Complex(0.0, 0.0)
                    state = StateOrTransition.Stabilizing
                }

                var noiseForConfidence = max(averageThroughputNoise, throughputErrorEstimate)
                if (noiseForConfidence > 0.0) {
                    confidence = (threadWaveComponent.abs() / noiseForConfidence) / targetSignalToNoiseRatio
                } else {
                    confidence = 1.0
                }
            }
        }

        var move = min(1.0, max(-1.0, ratio.re))
        move *= min(1.0, max(0.0, confidence))

        var gain = maxChangePerSecond * sampleDurationSeconds
        move = abs(move).pow(gainExponent) * (if (move >= 0.0) 1.0 else -1.0) * gain
        move = min(move, maxChangePerSample.toDouble())

        // TODO: if move > 0 and there is high cpu utilization, don't make a move

        currentControlSetting += move

        var newThreadWaveMagnitude = (0.5 + (currentControlSetting * averageThroughputNoise * targetSignalToNoiseRatio * threadMagnitudeMultiplier * 2.0)).toInt()
        newThreadWaveMagnitude = min(newThreadWaveMagnitude, maxThreadWaveMagnitude)
        newThreadWaveMagnitude = max(newThreadWaveMagnitude, 1)

        // TODO - get max/min threads from scheduler
        var maxThreads = 64
        var minThreads = 1

        currentControlSetting = min(currentControlSetting, (maxThreads - newThreadWaveMagnitude).toDouble())
        currentControlSetting = max(currentControlSetting, minThreads.toDouble())

        var newThreadCount = (currentControlSetting + newThreadWaveMagnitude * ((totalSamples / (wavePeriod / 2)) % 2)).toInt()

        newThreadCount = min(maxThreads, newThreadCount)
        newThreadCount = max(minThreads, newThreadCount)

        // TODO - log here if needed
        System.err.println("$currentControlSetting, $newThreadCount, $move, $gain")

        if (newThreadCount != currentThreadCount) {
            changeThreadCount(newThreadCount, state)
            secondsElapsedSinceLastChange = 0.0
            completionsSinceLastChange = 0.0
        }

        var newSampleInterval: Int
        if (ratio.re < 0.0 && newThreadCount == minThreads) {
            newSampleInterval = (0.5 + currentSampleMs * (10.0 * min(-ratio.re, 1.0))).toInt()
        } else {
            newSampleInterval = currentSampleMs
        }

        return (newThreadCount to newSampleInterval)
    }

    private fun changeThreadCount(newThreadCount: Int, state: StateOrTransition) {
        lastThreadCount = newThreadCount
        if (state != StateOrTransition.CooperativeBlocking) {
            currentSampleMs = Random.nextInt(sampleIntervalMsLow, sampleIntervalMsHigh + 1)
        }
    }

    private fun forceChange(newThreadCount: Int, state: StateOrTransition) {
        if (lastThreadCount != newThreadCount) {
            currentControlSetting += newThreadCount - lastThreadCount
            changeThreadCount(newThreadCount, state)
        }
    }

    private fun getWaveComponent(samples: DoubleArray, numSamples: Int, period: Double): Complex {
        require(numSamples >= period)
        require(period >= 2)
        require(numSamples <= samples.size)

        var w = 2 * PI / period
        var cosine = cos(w)
        var coeff = 2 * cosine
        var q0: Double
        var q1 = 0.0
        var q2 = 0.0
        for (i in 0..(numSamples - 1)) {
            q0 = coeff * q1 - q2 + samples[((totalSamples - numSamples + i) % samplesToMeasure).toInt()]
            q2 = q1
            q1 = q0
        }

        return Complex(q1 - q2 * cosine, q2 * sin(w)) / numSamples.toDouble()
    }
}