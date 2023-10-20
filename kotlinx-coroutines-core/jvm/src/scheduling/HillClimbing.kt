/*
 * Copyright 2016-2023 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.scheduling

import kotlin.random.Random
import kotlin.math.*

internal class HillClimbing(
    private val scheduler: DotnetBasedCoroutineScheduler,
    private val config: Config = DefaultConfig()
) {
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
    

    private val samples = DoubleArray(config.samplesToMeasure)
    private val threadCounts = DoubleArray(config.samplesToMeasure)
    private var currentSampleMs = Random.nextInt(config.sampleIntervalMsLow, config.sampleIntervalMsHigh + 1)
    private var lastThreadCount = 0
    private var accumulatedSampleDurationSeconds = 0.0
    private var accumulatedCompletionCount = 0
    private var totalSamples = 0L
    private var averageThroughputNoise = 0.0
    private var currentControlSetting = 0.0
    private var secondsElapsedSinceLastChange = 0.0
    private var completionsSinceLastChange = 0

    fun update(currentThreadCount: Int, pSampleDurationSeconds: Double, pNumCompletions: Int): Pair<Int, Int> {
        // If someone changed the thread count without telling us, update our records accordingly.
        if (currentThreadCount != lastThreadCount) {
            forceChange(currentThreadCount, StateOrTransition.Initializing)
        }

        // Update the cumulative stats for this thread count.
        secondsElapsedSinceLastChange += pSampleDurationSeconds
        completionsSinceLastChange += pNumCompletions

        // Add in any data we've already collected about this sample.
        val sampleDurationSeconds = pSampleDurationSeconds + accumulatedSampleDurationSeconds
        val numCompletions = pNumCompletions + accumulatedCompletionCount

        // We need to make sure we're collecting reasonably accurate data. Since we're just counting the end
        // of each work item, we are going to be missing some data about what really happened during the
        // sample interval. The count produced by each thread includes an initial work item that may have
        // started well before the start of the interval, and each thread may have been running some new
        // work item for some time before the end of the interval, which did not yet get counted. So
        // our count is going to be off by +/- threadCount work items.
        //
        // The exception is that the thread that reported to us last time definitely wasn't running any work
        // at that time, and the thread that's reporting now definitely isn't running a work item now. So
        // we really only need to consider threadCount-1 threads.
        //
        // Thus, the percent error in our count is +/- (threadCount-1)/numCompletions.
        //
        // We cannot rely on the frequency-domain analysis we'll be doing later to filter out this error, because
        // of the way it accumulates over time. If this sample is off by, say, 33% in the negative direction,
        // then the next one likely will be too. The one after that will include the sum of the completions
        // we missed in the previous samples, and so will be 33% positive.  So every three samples we'll have
        // two "low" samples and one "high" sample. This will appear as periodic variation right in the frequency
        // range we're targeting, which will not be filtered by the frequency-domain translation.
        if (totalSamples > 0 && ((currentThreadCount - 1.0) / numCompletions) >= config.maxSampleError) {
            // not accurate enough yet. Let's accumulate the data so far, and tell the scheduler
            // to collect a little more.
            accumulatedSampleDurationSeconds = sampleDurationSeconds
            accumulatedCompletionCount = numCompletions
            return (currentThreadCount to 10)
        }

        // We've got enough data for our sample. Reset our accumulators for next time.
        accumulatedSampleDurationSeconds = 0.0
        accumulatedCompletionCount = 0

        // Add the current thread count and throughput sample to our history.
        val throughput = numCompletions / sampleDurationSeconds
        val sampleIndex = (totalSamples % config.samplesToMeasure).toInt()
        samples[sampleIndex] = throughput
        threadCounts[sampleIndex] = currentThreadCount.toDouble()
        totalSamples++

        var ratio = Complex(0.0, 0.0)
        var confidence = 0.0
        var state = StateOrTransition.Warmup

        // How many samples will we use? It must be at least the three wave periods we're looking for, and it must also be a whole
        // multiple of the primary wave's period; otherwise the frequency we're looking for will fall between two frequency bands
        // in the Fourier analysis, and we won't be able to measure it accurately.
        val sampleCount = min(totalSamples - 1, config.samplesToMeasure.toLong()).toInt() / config.wavePeriod * config.wavePeriod

        if (sampleCount > config.wavePeriod) {
            // Average the throughput and thread count samples, so we can scale the wave magnitudes later.
            var sampleSum = 0.0
            var threadSum = 0.0

            for (i in 0 until sampleCount) {
                sampleSum += samples[((totalSamples - sampleCount + i) % config.samplesToMeasure).toInt()]
                threadSum += threadCounts[((totalSamples - sampleCount + i) % config.samplesToMeasure).toInt()]
            }

            val averageThroughput = sampleSum / sampleCount
            val averageThreadCount = threadSum / sampleCount

            if (averageThroughput > 0 && averageThreadCount > 0) {
                // Calculate the periods of the adjacent frequency bands we'll be using to measure noise levels.
                // We want the two adjacent Fourier frequency bands.
                val adjacentPeriod1 = sampleCount / ((sampleCount.toDouble() / config.wavePeriod) + 1)
                val adjacentPeriod2 = sampleCount / ((sampleCount.toDouble() / config.wavePeriod) - 1)

                // Get the three different frequency components of the throughput (scaled by average
                // throughput). Our "error" estimate (the amount of noise that might be present in the
                // frequency band we're really interested in) is the average of the adjacent bands.
                val throughputWaveComponent = getWaveComponent(samples, sampleCount, config.wavePeriod.toDouble()) / averageThroughput
                var throughputErrorEstimate = (getWaveComponent(samples, sampleCount, adjacentPeriod1) / averageThroughput).abs()

                if (adjacentPeriod2 <= sampleCount) {
                    throughputErrorEstimate = max(throughputErrorEstimate, (getWaveComponent(samples, sampleCount, adjacentPeriod2) / averageThroughput).abs())
                }

                // Do the same for the thread counts, so we have something to compare to. We don't measure thread count
                // noise, because there is none. These are exact measurements.
                val threadWaveComponent = getWaveComponent(threadCounts, sampleCount, config.wavePeriod.toDouble()) / averageThreadCount

                // Update our moving average of the throughput noise. We'll use this later as feedback to
                // determine the new size of the thread wave.
                averageThroughputNoise = if (averageThroughputNoise == 0.0) {
                    throughputErrorEstimate
                } else {
                    (config.throughputErrorSmoothingFactor * throughputErrorEstimate) + ((1.0 - config.throughputErrorSmoothingFactor) * averageThroughputNoise)
                }

                if (threadWaveComponent.abs() > 0.0) {
                    // Adjust the throughput wave, so it's centered around the target wave, and then calculate the adjusted throughput/thread ratio.
                    ratio = (throughputWaveComponent - (threadWaveComponent * config.targetThroughputRatio)) / threadWaveComponent
                    state = StateOrTransition.ClimbingMove
                } else {
                    ratio = Complex(0.0, 0.0)
                    state = StateOrTransition.Stabilizing
                }

                // Calculate how confident we are in the ratio. More noise == less confident. This has
                // the effect of slowing down movements that might be affected by random noise.
                val noiseForConfidence = max(averageThroughputNoise, throughputErrorEstimate)

                confidence = if (noiseForConfidence > 0.0) {
                    (threadWaveComponent.abs() / noiseForConfidence) / config.targetSignalToNoiseRatio
                } else {
                    1.0 // There is no noise!
                }
            }
        }

        // We use just the real part of the complex ratio we just calculated. If the throughput signal
        // is exactly in phase with the thread signal, this will be the same as taking the magnitude of
        // the complex move and moving that far up. If they're 180 degrees out of phase, we'll move
        // backward (because this indicates that our changes are having the opposite of the intended effect).
        // If they're 90 degrees out of phase, we won't move at all, because we can't tell whether we're
        // having a negative or positive effect on throughput.
        var move = min(1.0, max(-1.0, ratio.re))

        // Apply our confidence multiplier.
        move *= min(1.0, max(0.0, confidence))

        // Now apply non-linear gain, such that values around zero are attenuated, while higher values
        // are enhanced. This allows us to move quickly if we're far away from the target, but more slowly
        // if we're getting close, giving us rapid ramp-up without wild oscillations around the target.
        val gain = config.maxChangePerSecond * sampleDurationSeconds
        move = abs(move).pow(config.gainExponent) * (if (move >= 0.0) 1.0 else -1.0) * gain
        move = min(move, config.maxChangePerSample.toDouble())

        // [TODO] If move > 0 and there is high cpu utilization, hillClimber shouldn't make a move.

        // Apply the move to our control setting.
        currentControlSetting += move

        // Calculate the new thread wave magnitude, which is based on the moving average we've been keeping of
        // the throughput error. This average starts at zero, so we'll start with a nice safe little wave at first.
        var newThreadWaveMagnitude = (0.5 + (currentControlSetting * averageThroughputNoise * config.targetSignalToNoiseRatio * config.threadMagnitudeMultiplier * 2.0)).toInt()
        newThreadWaveMagnitude = min(newThreadWaveMagnitude, config.maxThreadWaveMagnitude)
        newThreadWaveMagnitude = max(newThreadWaveMagnitude, 1)

        // Make sure our control setting is within the scheduler's limits. When some threads are blocked due to
        // cooperative blocking, ensure that hill climbing does not decrease the thread count below the expected
        // minimum.
        val maxThreads = scheduler.maxPoolSize
        val minThreads = scheduler.minThreadsGoal

        currentControlSetting = min(currentControlSetting, (maxThreads - newThreadWaveMagnitude).toDouble())
        currentControlSetting = max(currentControlSetting, minThreads.toDouble())

        // Calculate the new thread count (control setting + square wave).
        var newThreadCount = (currentControlSetting + newThreadWaveMagnitude * ((totalSamples / (config.wavePeriod / 2)) % 2)).toInt()

        // Make sure the new thread count doesn't exceed the scheduler's limits.
        newThreadCount = min(maxThreads, newThreadCount)
        newThreadCount = max(minThreads, newThreadCount)

        // If all of this caused an actual change in thread count, log that.
        if (newThreadCount != currentThreadCount) {
            changeThreadCount(newThreadCount, state)
            secondsElapsedSinceLastChange = 0.0
            completionsSinceLastChange = 0
        }

        // Return the new thread count and sample interval. This is randomized to prevent correlations with other periodic
        // changes in throughput. Among other things, this prevents us from getting confused by Hill Climbing instances
        // running in other processes.
        //
        // If we're at minThreads, and we seem to be hurting performance by going higher, we can't go any lower to fix this. So
        // we'll simply stay at minThreads much longer, and only occasionally try a higher value.
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
            currentSampleMs = Random.nextInt(config.sampleIntervalMsLow, config.sampleIntervalMsHigh + 1)

            if (LOG_MAJOR_HC_ADJUSTMENTS) {
                val throughput =
                    if (secondsElapsedSinceLastChange > 0.0) completionsSinceLastChange.toDouble() / secondsElapsedSinceLastChange else 0.0
                System.err.println("HC: [$newThreadCount, $throughput, $state]")
            }
        }
    }

    private fun getWaveComponent(samples: DoubleArray, numSamples: Int, period: Double): Complex {
        require(numSamples >= period)
        require(period >= 2)
        require(numSamples <= samples.size)

        // Calculate the sinusoid with the given period.
        // We're using the Goertzel algorithm for this. See http://en.wikipedia.org/wiki/Goertzel_algorithm.
        val w = 2.0 * PI / period
        val cosine = cos(w)
        val sine = sin(w)
        val coeff = 2.0 * cosine
        var q0: Double
        var q1 = 0.0
        var q2 = 0.0

        for (i in 0 until numSamples) {
            q0 = coeff * q1 - q2 + samples[((totalSamples - numSamples + i) % config.samplesToMeasure).toInt()]
            q2 = q1
            q1 = q0
        }

        return Complex(q1 - q2 * cosine, q2 * sine) / numSamples.toDouble()
    }

    abstract class Config {
        abstract val wavePeriod: Int
        abstract val maxThreadWaveMagnitude: Int
        abstract val threadMagnitudeMultiplier: Double
        abstract val samplesToMeasure: Int
        abstract val targetThroughputRatio: Double
        abstract val targetSignalToNoiseRatio: Double
        abstract val maxChangePerSecond: Int
        abstract val maxChangePerSample: Int
        abstract val sampleIntervalMsLow: Int
        abstract val sampleIntervalMsHigh: Int
        abstract val throughputErrorSmoothingFactor: Double
        abstract val gainExponent: Double
        abstract val maxSampleError: Double
    }

    open class DefaultConfig : Config() {
        override val wavePeriod = 4
        override val maxThreadWaveMagnitude = 20
        override val threadMagnitudeMultiplier = 100.0 / 100.0
        override val samplesToMeasure = 32 // must be divisible by wavePeriod
        override val targetThroughputRatio = 15.0 / 100.0
        override val targetSignalToNoiseRatio = 300.0 / 100.0
        override val maxChangePerSecond = 4
        override val maxChangePerSample = 20
        override val sampleIntervalMsLow = 10
        override val sampleIntervalMsHigh = 200
        override val throughputErrorSmoothingFactor = 1.0 / 100.0
        override val gainExponent = 200.0 / 100.0
        override val maxSampleError = 15.0 / 100.0
    }

    open class LinearGain(pMaxChangePerSecond: Int = 4) : DefaultConfig() {
        override val gainExponent = 100.0 / 100.0
        override val maxChangePerSecond = pMaxChangePerSecond
    }
}