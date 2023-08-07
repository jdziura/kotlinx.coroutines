/*
 * Copyright 2016-2023 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.scheduling

import kotlinx.atomicfu.*
import kotlinx.coroutines.internal.*
import kotlinx.coroutines.*
import java.io.*
import java.lang.Runnable
import java.util.Stack
import java.util.concurrent.*
import java.util.concurrent.atomic.*
import java.util.concurrent.locks.*
import java.util.concurrent.locks.LockSupport.*
import kotlin.concurrent.*
import kotlin.jvm.internal.*
import kotlin.random.*
import kotlin.math.*
import kotlin.random.Random

internal const val SCHED_DEBUG = false
internal fun schedDebug(msg: String) {
    if (SCHED_DEBUG)
        System.err.println(msg)
}

internal fun runSafely(task: Task) {
    try {
        task.run()
    } catch (e: Throwable) {
        val thread = Thread.currentThread()
        thread.uncaughtExceptionHandler.uncaughtException(thread, e)
    } finally {
        unTrackTask()
    }
}

internal class CsBasedCoroutineScheduler(
    @JvmField val corePoolSize: Int,
    @JvmField val maxPoolSize: Int,
    @JvmField val schedulerName: String = DEFAULT_SCHEDULER_NAME
) : Scheduler {
    init {
        require(corePoolSize >= CoroutineScheduler.MIN_SUPPORTED_POOL_SIZE) {
            "Core pool size $corePoolSize should be at least ${CoroutineScheduler.MIN_SUPPORTED_POOL_SIZE}"
        }
        require(maxPoolSize >= corePoolSize) {
            "Max pool size $maxPoolSize should be greater than or equals to core pool size $corePoolSize"
        }
        require(maxPoolSize <= CoroutineScheduler.MAX_SUPPORTED_POOL_SIZE) {
            "Max pool size $maxPoolSize should not exceed maximal supported number of threads ${CoroutineScheduler.MAX_SUPPORTED_POOL_SIZE}"
        }
    }

    companion object {
        private const val MAX_RUNS = 2
        private const val GATE_THREAD_RUNNING_MASK = 0x4
        private const val GATE_ACTIVITIES_PERIOD_MS = DelayHelper.GATE_ACTIVITIES_PERIOD_MS
        private const val DELAY_STEP_MS = 25L
        private const val MAX_DELAY_MS = 250L
//        private const val MAX_THREADS = CoroutineScheduler.MAX_SUPPORTED_POOL_SIZE
        private const val MIN_THREADS = CoroutineScheduler.MIN_SUPPORTED_POOL_SIZE
        private const val THREAD_TIMEOUT_MS = 20 * 1000L
        private const val DISPATCH_QUANTUM_MS = 30L
    }

    private val MAX_THREADS = maxPoolSize

    private val numProcessors = Runtime.getRuntime().availableProcessors()
    private val threadsToAddWithoutDelay = numProcessors
    private val threadsPerDelayStep = numProcessors

    private val numRequestedWorkers = atomic(0)
    private val counts = ThreadCounts(corePoolSize, this)

    private var currentSampleStartTime = 0L
    private var threadAdjustmentIntervalMs = 0
    private var completionCount = 0
    private var priorCompletionCount = 0

    // TODO - verify bounds
    private val hillClimber = HillClimbing(corePoolSize, maxPoolSize)
    private var nextCompletedWorkRequestsTime = 0L
    private var priorCompletedWorkRequestTime = 0L

    private val _isTerminated = atomic(false)
    val isTerminated: Boolean get() = _isTerminated.value
    private val workerStack = Stack<Worker>()

    private var gateThreadRunningState = atomic(0)
    private val runGateThreadEvent = AutoResetEvent(true)
    private val delayEvent = AutoResetEvent(false)
    private val delayHelper = DelayHelper()
    private val lastDequeueTime = atomic(0L)

    private val semaphore = Semaphore(0)

    private var numBlockingTasks = 0
    private var numThreadsAddedDueToBlocking = 0
    private var createdWorkers = 0

    @JvmField
    val workers = ResizableAtomicArray<CsBasedCoroutineScheduler.Worker>((corePoolSize + 1) * 2)

    @JvmField
    val globalQueue = GlobalQueue()

    private var hasOutstandingThreadRequest = atomic(0)

    private val minThreadsGoal: Int
        get() {
            return min(counts.numThreadsGoal, targetThreadsForBlockingAdjustment)
        }

    private val targetThreadsForBlockingAdjustment: Int
        get() {
            return if (numBlockingTasks <= 0) {
                corePoolSize
            } else {
                min(corePoolSize + numBlockingTasks, MAX_THREADS)
            }
        }

    private var pendingBlockingAdjustment = PendingBlockingAdjustment.None

    private enum class PendingBlockingAdjustment {
        None,
        Immediately,
        WithDelayIfNecessary
    }

    internal inner class Worker private constructor() : Thread() {
        init {
            isDaemon = true
        }

        var indexInArray = 0
            set(index) {
                name = "$schedulerName-worker-${if (index == 0) "TERMINATED" else index.toString()}"
                field = index
            }

        constructor(index: Int): this() {
            indexInArray = index
        }

        val inStack = atomic(false)

        @JvmField
        val localQueue: WorkQueue = WorkQueue()

        private var rngState = Random.nextInt()

        private val stolenTask: Ref.ObjectRef<Task?> = Ref.ObjectRef()

        inline val scheduler get() = this@CsBasedCoroutineScheduler

        override fun run() = runWorker()

        private fun runWorker() {
            schedDebug("[$name] created")
            while (!isTerminated) {
                while (!isTerminated) {
                    try {
                        if (semaphore.tryAcquire(THREAD_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                            doWork()
                        } else {
                            break
                        }
                    } catch (e: InterruptedException) {
                        this.interrupt()
                        break
                    }
                }
                if (shouldExitWorker()) {
                    tryPark()
                }
            }
        }

        private fun doWork() {
            schedDebug("[$name] doWork()")
            var alreadyRemovedWorkingWorker = false
            while (takeActiveRequest()) {
                lastDequeueTime.getAndSet(System.currentTimeMillis())
                if (!dispatchFromQueue()) {
                    alreadyRemovedWorkingWorker = true
                    break
                }

                if (numRequestedWorkers.value <= 0) {
                    break
                }

                yield()
            }

            if (!alreadyRemovedWorkingWorker) {
                removeWorkingWorker()
            }
        }

        private fun dequeue(globalFirst: Boolean = false): Task? {
            if (globalFirst) pollGlobalQueues()?.let { return it }
            localQueue.poll()?.let { return it }
            if (!globalFirst) pollGlobalQueues()?.let { return it }

            val (task, missedSteal) = trySteal(STEAL_ANY)
            if (missedSteal) {
                ensureThreadRequested()
            }
            return task
        }

        private fun shouldExitWorker(): Boolean {
            synchronized(this@CsBasedCoroutineScheduler) {
                if (counts.numExistingThreads <= counts.numProcessingWork) {
                    return false
                }

                counts.numExistingThreads--
                counts.numThreadsGoal = max(minThreadsGoal, min(counts.numExistingThreads, counts.numThreadsGoal))
                hillClimber.forceChange(counts.numThreadsGoal, HillClimbing.StateOrTransition.ThreadTimedOut)
                return true
            }
        }

        private fun tryPark() {
            if (inStack.getAndSet(true) == false) {
                synchronized(workerStack) {
                    workerStack.push(this)
                }
            }

            while (inStack.value == true) {
                if (isTerminated) break
                interrupted()
                schedDebug("[$name] park()")
                park()
            }
        }

        fun nextInt(upperBound: Int): Int {
            var r = rngState
            r = r xor (r shl 13)
            r = r xor (r shr 17)
            r = r xor (r shl 5)
            rngState = r
            val mask = upperBound - 1
            // Fast path for power of two bound
            if (mask and upperBound == 0) {
                return r and mask
            }
            return (r and Int.MAX_VALUE) % upperBound
        }

        private fun dispatchFromQueue(): Boolean {
            markThreadRequestSatisifed()

            var workItem: Task? = dequeue(globalFirst = true) ?: return true
            var startTickCount = System.currentTimeMillis()

            while (true) {
                if (workItem == null) {
                    workItem = dequeue() ?: return true
                }

                executeWorkItem(workItem)
                workItem = null

                val currentTickCount = System.currentTimeMillis()
                if (!notifyWorkItemComplete(currentTickCount)) {
                    return false
                }

                if (currentTickCount - startTickCount < DISPATCH_QUANTUM_MS) {
                    continue
                }

                startTickCount = currentTickCount
            }
        }

        private fun pollGlobalQueues(): Task? {
            return globalQueue.removeFirstOrNull()
        }

        // TODO - rewrite to not use pair, and use better check for missedSteal
        private fun trySteal(stealingMode: StealingMode): Pair<Task?, Boolean> {
            val created: Int
            synchronized(scheduler) { created = createdWorkers }

            if (created < 2) {
                return (null to false)
            }

            var missedSteal = false
            var currentIndex = nextInt(created)
            repeat(created) {
                ++currentIndex
                if (currentIndex > created) currentIndex = 1
                val worker = workers[currentIndex]
                if (worker !== null && worker !== this) {
                    val stealResult = worker.localQueue.trySteal(stealingMode, stolenTask)
                    if (stealResult == TASK_STOLEN) {
                        val result = stolenTask.element
                        stolenTask.element = null
                        return (result to false)
                    } else if (worker.localQueue.size > 0) {
                        missedSteal = true
                    }
                }
            }

            return (null to missedSteal)
        }
    }

    internal inner class GateThread : Thread() {
        init {
            isDaemon = true
            name = "$schedulerName-gateThread"
        }

        override fun run() = runGateThread()

        private fun runGateThread() {
            val disableStarvationDetection = false
            while (!isTerminated) {
                runGateThreadEvent.waitOne()
                var currentTimeMs = System.currentTimeMillis()
                delayHelper.setGateActivitiesTime(currentTimeMs)

                while (!isTerminated) {
                    val wasSignaledToWake = delayEvent.waitOne(delayHelper.getNextDelay(currentTimeMs))
                    currentTimeMs = System.currentTimeMillis()

                    do {
                        if (pendingBlockingAdjustment == PendingBlockingAdjustment.None) {
                            delayHelper.clearBlockingAdjustmentDelay()
                            break
                        }
                        var previousDelayElapsed = false
                        if (delayHelper.hasBlockingAdjustmentDelay) {
                            previousDelayElapsed =
                                delayHelper.hasBlockingAdjustmentDelayElapsed(currentTimeMs, wasSignaledToWake)

                            if (pendingBlockingAdjustment == PendingBlockingAdjustment.WithDelayIfNecessary &&
                                !previousDelayElapsed) {
                                break
                            }
                        }

                        val nextDelayMs = performBlockingAdjustment(previousDelayElapsed)
                        if (nextDelayMs <= 0) {
                            delayHelper.clearBlockingAdjustmentDelay()
                        } else {
                            delayHelper.setBlockingAdjustmentTimeAndDelay(currentTimeMs, nextDelayMs)
                        }
                    } while (false)

                    if (!delayHelper.shouldPerformGateActivities(currentTimeMs, wasSignaledToWake)) {
                        continue
                    }

                    // TODO - here log CPU utilization if possible

                    if (!disableStarvationDetection &&
                        pendingBlockingAdjustment == PendingBlockingAdjustment.None &&
                        numRequestedWorkers.value > 0 &&
                        sufficientDelaySinceLastDequeue()) {

                        var addWorker = false
                        synchronized(this@CsBasedCoroutineScheduler) {
                            if (counts.numProcessingWork < MAX_THREADS &&
                                counts.numProcessingWork >= counts.numThreadsGoal) {

                                counts.numThreadsGoal = counts.numProcessingWork + 1
                                hillClimber.forceChange(counts.numThreadsGoal, HillClimbing.StateOrTransition.Starvation)
                                addWorker = true
                            }
                        }

                        if (addWorker) {
                            maybeAddWorker()
                        }
                    }

                    if (numRequestedWorkers.value <= 0 &&
                        pendingBlockingAdjustment == PendingBlockingAdjustment.None &&
                        gateThreadRunningState.decrementAndGet() <= getRunningStateForNumRuns(0)) {

                        break
                    }
                }
            }
        }
    }

    override fun dispatch(block: Runnable, taskContext: TaskContext, tailDispatch: Boolean) {
        schedDebug("[$schedulerName] dispatch()")
        trackTask()
        val task = createTask(block, taskContext)
        enqueue(task, tailDispatch)
    }

    override fun createTask(block: Runnable, taskContext: TaskContext): Task {
        val nanoTime = schedulerTimeSource.nanoTime()
        if (block is Task) {
            block.submissionTime = nanoTime
            block.taskContext = taskContext
            return block
        }
        return TaskImpl(block, nanoTime, taskContext)
    }

    override fun execute(command: Runnable) = dispatch(command)

    override fun close() {
        shutdown(10_000L)
    }

    override fun shutdown(timeout: Long) {
        if (!_isTerminated.compareAndSet(false, true)) return

        // Wakes gateThread if needed, should terminate afterwards
        delayEvent.set()
        runGateThreadEvent.set()

        semaphore.release(MAX_THREADS)

        val currentWorker = currentWorker()
        while (true) {
            val worker: Worker?
            synchronized(workerStack) {
                worker = if (workerStack.empty()) null else workerStack.pop()
            }
            if (worker == null) break
            if (worker === currentWorker) continue
            while (worker.isAlive) {
                LockSupport.unpark(worker)
                worker.join(timeout)
            }
        }
    }

    private fun enqueue(task: Task, tailDispatch: Boolean) {
        val currentWorker = currentWorker()
        val notAdded = currentWorker.submitToLocalQueue(task, tailDispatch)
        if (notAdded != null) {
            if (!addToGlobalQueue(notAdded)) {
                // Global queue is closed in the last step of close/shutdown -- no more tasks should be accepted
                throw RejectedExecutionException("$schedulerName was terminated")
            }
        }
        ensureThreadRequested()
    }

    private fun executeWorkItem(workItem: Task) {
        beforeTask(workItem.mode)
        runSafely(workItem)
        afterTask(workItem.mode)
    }

    private fun markThreadRequestSatisifed() {
        hasOutstandingThreadRequest.getAndSet(0)
    }

    private fun Worker?.submitToLocalQueue(task: Task, tailDispatch: Boolean): Task? {
        if (this == null) return task
        if (task.isBlocking) return task
        if (isTerminated) return task
        return localQueue.add(task, fair = tailDispatch)
    }

    private fun addToGlobalQueue(task: Task): Boolean {
        return globalQueue.addLast(task)
    }

    private fun ensureThreadRequested() {
        // TODO - check if other fix possible
        // Scenario not working for now:
        // 100 blocking tasks launched, since contention some fail this if and ~30 get created, since
        // blocking adjustment adds at most 1 thread at a time
        // possible other fix - add more threads at a time during blocking adjustment
//        if (hasOutstandingThreadRequest.compareAndSet(0, 1)) {
            requestWorker()
//        }
    }

    private fun currentWorker(): Worker? = (Thread.currentThread() as? Worker)?.takeIf { it.scheduler == this }

    private fun tryUnpark(): Boolean {
        synchronized(workerStack) {
            if (workerStack.empty()) return false
            val worker = workerStack.pop()
            worker.inStack.getAndSet(false)
            schedDebug("${worker.name} unpark()")
            LockSupport.unpark(worker)
            return true
        }
    }

    private fun requestWorker() {
        schedDebug("[$schedulerName] requestWorker()")
        numRequestedWorkers.incrementAndGet()
        maybeAddWorker()
        ensureGateThreadRunning()
    }

    fun notifyWorkItemComplete(currentTimeMs: Long): Boolean {
        notifyWorkItemProgress(currentTimeMs)
        return !shouldStopProcessingWorkNow()
    }

    private fun notifyWorkItemProgress(currentTimeMs: Long) {
        lastDequeueTime.getAndSet(currentTimeMs)
        if (shouldAdjustMaxWorkersActive(currentTimeMs)) {
            adjustMaxWorkersActive()
        }
    }

    private fun adjustMaxWorkersActive() {
        var addWorker = false
        synchronized(this) {
            if (counts.numProcessingWork > counts.numThreadsGoal ||
                pendingBlockingAdjustment != PendingBlockingAdjustment.None) {
                return
            }

            val currentTicks = System.currentTimeMillis()
            val elapsedMs = currentTicks - currentSampleStartTime

            if (elapsedMs >= threadAdjustmentIntervalMs / 2) {
                val numCompletions = completionCount - priorCompletionCount
                val oldNumThreadsGoal = counts.numThreadsGoal
                val updateResult = hillClimber.update(oldNumThreadsGoal, elapsedMs / 1000.0, numCompletions)
                val newNumThreadsGoal = updateResult.first
                threadAdjustmentIntervalMs = updateResult.second

                if (oldNumThreadsGoal != newNumThreadsGoal) {
                    counts.numThreadsGoal = newNumThreadsGoal
                    if (newNumThreadsGoal > oldNumThreadsGoal) {
                        addWorker = true
                    }
                }

                priorCompletionCount = numCompletions
                nextCompletedWorkRequestsTime = currentTicks + threadAdjustmentIntervalMs
                priorCompletedWorkRequestTime = currentTicks
                currentSampleStartTime = currentTicks
            }
        }
        if (addWorker) {
            maybeAddWorker()
        }
    }

    private fun shouldStopProcessingWorkNow(): Boolean {
        synchronized(this) {
            if (counts.numProcessingWork <= counts.numThreadsGoal) {
                return false
            }

            counts.numProcessingWork--
            return true
        }
    }

    private fun shouldAdjustMaxWorkersActive(currentTimeMs: Long): Boolean {
        synchronized(this) {
            val priorTime = priorCompletedWorkRequestTime
            val requiredInterval = nextCompletedWorkRequestsTime - priorTime
            val elapsedInterval = currentTimeMs - priorTime

            if (elapsedInterval < requiredInterval) {
                return false
            }

            if (counts.numProcessingWork > counts.numThreadsGoal) {
                return false
            }

            return pendingBlockingAdjustment == PendingBlockingAdjustment.None
        }
    }

    private fun maybeAddWorker() {
        schedDebug("[$schedulerName] maybeAddWorker()")
        val toCreate: Int
        val toRelease: Int

        synchronized(this) {
            if (counts.numProcessingWork >= counts.numThreadsGoal) {
                return
            }

            val newNumProcessingWork = if (numBlockingTasks > 0) {
                max(counts.numProcessingWork + 1, targetThreadsForBlockingAdjustment)
            } else {
                counts.numProcessingWork + 1
            }
//            val newNumProcessingWork = max(counts.numProcessingWork + 1, targetThreadsForBlockingAdjustment)

            val newNumExistingThreads = max(counts.numExistingThreads, newNumProcessingWork)
            toRelease = newNumProcessingWork - counts.numProcessingWork
            toCreate = newNumExistingThreads - counts.numExistingThreads
            counts.numExistingThreads = newNumExistingThreads
            counts.numProcessingWork = newNumProcessingWork
        }

        semaphore.release(toRelease)

        repeat(toCreate) {
            if (!tryUnpark()) {
                createWorker()
            }
        }
    }

    private fun createWorker() {
        val worker: Worker
        synchronized(this) {
            if (isTerminated) return
            val newIndex = createdWorkers + 1
            require(newIndex > 0 && workers[newIndex] == null)
            worker = Worker(newIndex)
            workers.setSynchronized(newIndex, worker)
            createdWorkers++
        }
        worker.start()
    }

    private fun removeWorkingWorker() {
        synchronized(this) {
            counts.numProcessingWork--
        }

        if (numRequestedWorkers.value > 0) {
            maybeAddWorker()
        }
    }

    private fun takeActiveRequest(): Boolean {
        var cnt = numRequestedWorkers.value
        while (cnt > 0) {
            if (numRequestedWorkers.compareAndSet(cnt, cnt - 1)) {
                return true
            }
            cnt = numRequestedWorkers.value
        }
        return false
    }

    private fun ensureGateThreadRunning() {
        if (gateThreadRunningState.value != getRunningStateForNumRuns(MAX_RUNS)) {
            ensureGateThreadRunningSlow()
        }
    }

    private fun ensureGateThreadRunningSlow() {
        val numRunsMask: Int = gateThreadRunningState.getAndSet(getRunningStateForNumRuns(MAX_RUNS))
        if (numRunsMask == getRunningStateForNumRuns(0)) {
            runGateThreadEvent.set()
        } else if ((numRunsMask and GATE_THREAD_RUNNING_MASK) == 0) {
            createGateThread()
        }
    }

    private fun createGateThread() {
        val gateThread = GateThread()
        gateThread.start()
    }

    private fun getRunningStateForNumRuns(numRuns: Int): Int {
        return GATE_THREAD_RUNNING_MASK or numRuns
    }

    private fun performBlockingAdjustment(previousDelayElapsed: Boolean): Long {
        val (nextDelayMs, addWorker) = performBlockingAdjustmentSync(previousDelayElapsed)

        if (addWorker) {
            maybeAddWorker()
        }

        return nextDelayMs
    }

    private fun performBlockingAdjustmentSync(previousDelayElapsed: Boolean): Pair<Long, Boolean> {
        var addWorker = false
        synchronized(this) {
            require(pendingBlockingAdjustment != PendingBlockingAdjustment.None)
            pendingBlockingAdjustment = PendingBlockingAdjustment.None
            val targetThreadsGoal = targetThreadsForBlockingAdjustment
            var numThreadsGoal = counts.numThreadsGoal

            if (numThreadsGoal == targetThreadsGoal) {
                return (0L to false)
            }

            if (numThreadsGoal > targetThreadsGoal) {
                if (numThreadsAddedDueToBlocking <= 0) {
                    return (0L to false)
                }

                val toSubtract = min(numThreadsGoal - targetThreadsGoal, numThreadsAddedDueToBlocking)
                numThreadsAddedDueToBlocking -= toSubtract
                numThreadsGoal -= toSubtract
                counts.numThreadsGoal = numThreadsGoal
                hillClimber.forceChange(numThreadsGoal, HillClimbing.StateOrTransition.CooperativeBlocking)

                return (0L to false)
            }

            // TODO - decide corePoolSize or MIN_THREADS
            val configuredMaxThreadsWithoutDelay = min(MIN_THREADS + threadsToAddWithoutDelay, MAX_THREADS)

            do {
                val maxThreadsGoalWithoutDelay = max(configuredMaxThreadsWithoutDelay, min(counts.numExistingThreads, MAX_THREADS))
                val targetThreadsGoalWithoutDelay = min(targetThreadsGoal, maxThreadsGoalWithoutDelay)
                val newNumThreadsGoal = if (numThreadsGoal < targetThreadsGoalWithoutDelay) {
                    targetThreadsGoalWithoutDelay
                } else if (previousDelayElapsed) {
                    numThreadsGoal + 1
                } else {
                    break
                }

                // TODO - handle memory usage here if needed

                numThreadsAddedDueToBlocking += newNumThreadsGoal - numThreadsGoal
                counts.numThreadsGoal = newNumThreadsGoal
                hillClimber.forceChange(newNumThreadsGoal, HillClimbing.StateOrTransition.CooperativeBlocking)

                if (counts.numProcessingWork >= numThreadsGoal && numRequestedWorkers.value > 0) {
                    addWorker = true
                }

                numThreadsGoal = newNumThreadsGoal
                if (numThreadsGoal >= targetThreadsGoal) {
                    return (0L to addWorker)
                }
            } while (false)

            pendingBlockingAdjustment = PendingBlockingAdjustment.WithDelayIfNecessary
            val delayStepCount = 1 + (numThreadsGoal - configuredMaxThreadsWithoutDelay) / threadsPerDelayStep
            return (min(delayStepCount * DELAY_STEP_MS, MAX_DELAY_MS) to addWorker)
        }
    }

    private fun wakeGateThread() {
        delayEvent.set()
        ensureGateThreadRunning()
    }

    private fun sufficientDelaySinceLastDequeue(): Boolean {
        val delay = System.currentTimeMillis() - lastDequeueTime.value

        // TODO - check for utilization
        val minimumDelay = GATE_ACTIVITIES_PERIOD_MS

        return delay > minimumDelay
    }

    // Function name may be confusing. What it does is inform gateThread about
    // start of potentially blocking task if needed. Name kept that way for now to navigate in C# code.
    // In ThreadPool, it is invoked when thread actually is blocked, here when task starts
    private fun notifyThreadBlocked() {
        var shouldWakeGateThread = false
        synchronized(this) {
            numBlockingTasks++
            require(numBlockingTasks > 0)

            if (pendingBlockingAdjustment != PendingBlockingAdjustment.WithDelayIfNecessary &&
                counts.numThreadsGoal < targetThreadsForBlockingAdjustment) {

                if (pendingBlockingAdjustment == PendingBlockingAdjustment.None) {
                    shouldWakeGateThread = true
                }

                pendingBlockingAdjustment = PendingBlockingAdjustment.WithDelayIfNecessary
            }

//            System.err.println("New blocking task, total: $numBlockingTasks\n" +
//                "Running Threads: ${counts.numProcessingWork}\n" +
//                "Existing Threads: ${counts.numExistingThreads}\n" +
//                "Goal Threads: ${counts.numThreadsGoal}\n" +
//                "Will wake up GateThread: $shouldWakeGateThread\n")
        }

        if (shouldWakeGateThread) {
            wakeGateThread()
        }
    }

    // Similar as above, just notifies completion of potentially blocking task.
    private fun notifyThreadUnblocked() {
        var shouldWakeGateThread = false
        synchronized(this) {
            numBlockingTasks--
            if (pendingBlockingAdjustment != PendingBlockingAdjustment.Immediately &&
                numThreadsAddedDueToBlocking > 0 &&
                counts.numThreadsGoal > targetThreadsForBlockingAdjustment) {

                shouldWakeGateThread = true
                pendingBlockingAdjustment = PendingBlockingAdjustment.Immediately
            }
        }

        if (shouldWakeGateThread) {
            wakeGateThread()
        }
    }

    private fun beforeTask(taskMode: Int) {
//        TODO - check if better to increment earlier or now (during dispatch())
        if (taskMode == TASK_PROBABLY_BLOCKING) {
            notifyThreadBlocked()
        }
    }

    private fun afterTask(taskMode: Int) {
        if (taskMode == TASK_PROBABLY_BLOCKING) {
            notifyThreadUnblocked()
        }
    }
}