/*
 * Copyright 2016-2021 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package benchmarks.scheduler

import benchmarks.*
import benchmarks.akka.*
import kotlinx.coroutines.*
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.annotations.State
import java.lang.Thread.*
import java.util.concurrent.*
import kotlin.concurrent.*
import kotlin.coroutines.*

@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
open class DispatchersContextSwitchBenchmark : ParametrizedDispatcherBase() {
    private val nCoroutines = 10000
    private val delayTimeMs = 1L
    private val nRepeatDelay = 10

    @Param("kotlin_default", "kotlin_prediction", "go", "dotnet_default", "dotnet_no_hc", "dotnet_linear_gain", "dotnet_linear_gain_fast", "fjp")
    override var dispatcher: String = "fjp"

    @Benchmark
    fun contextSwitchBenchmark() = runBenchmark(coroutineContext)

    private fun runBenchmark(dispatcher: CoroutineContext)  = runBlocking {
        repeat(nCoroutines) {
            launch(dispatcher) {
                repeat(nRepeatDelay) {
                    delay(delayTimeMs)
                }
            }
        }
    }
}

