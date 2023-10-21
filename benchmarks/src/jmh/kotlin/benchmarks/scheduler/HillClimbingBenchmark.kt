/*
 * Copyright 2016-2023 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package benchmarks.scheduler

import benchmarks.*
import benchmarks.common.*
import kotlinx.coroutines.*
import org.openjdk.jmh.annotations.*
import java.util.concurrent.*


@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 20, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
open class HillClimbingBenchmark : ParametrizedDispatcherBase() {

    @Param("kotlin_default", "kotlin_prediction", "go", "dotnet_default", "dotnet_no_hc", "dotnet_linear_gain", "dotnet_linear_gain_fast", "fjp")
    override var dispatcher: String = "fjp"

    private val tasksWidth = 100
    private val tasksDepth = 10
    private val taskLenMinMs = 2L
    private val taskLenMaxMs = 5L

    @Benchmark
    fun hillClimbingBenchmark() {
        val latch = CountDownLatch(tasksWidth)
        val jobs = ArrayList<Job>()

        for (i in 1..tasksWidth) {
            jobs += GlobalScope.launch(coroutineContext) {
                repeat(tasksDepth) {
                    launch(coroutineContext) {
                        val time = ThreadLocalRandom.current().nextLong(taskLenMinMs, taskLenMaxMs + 1)
                        doGeomDistrWork(50)
                        Thread.sleep(time)
                        doGeomDistrWork(50)
                    }.join()
                }

                latch.countDown()
            }
        }

        latch.await()
    }
}