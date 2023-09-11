/*
 * Copyright 2016-2021 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package benchmarks.flow

import benchmarks.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import org.openjdk.jmh.annotations.*
import java.util.concurrent.*
import benchmarks.flow.scrabble.flow as unsafeFlow
import kotlinx.coroutines.flow.flow as safeFlow

@Warmup(iterations = 7, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 7, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
open class SafeFlowBenchmark : ParametrizedDispatcherBase() {
    @Param("scheduler", "fjp", "go_scheduler", "dotnet_scheduler")
    override var dispatcher: String = "fjp"

    private fun numbersSafe() = safeFlow {
        for (i in 2L..1000L) emit(i)
    }

    private fun numbersUnsafe() = unsafeFlow {
        for (i in 2L..1000L) emit(i)
    }

    @Benchmark
    fun safeNumbers(): Int = runBlocking {
        numbersSafe().count()
    }

    @Benchmark
    fun unsafeNumbers(): Int = runBlocking {
        numbersUnsafe().count()
    }
}
