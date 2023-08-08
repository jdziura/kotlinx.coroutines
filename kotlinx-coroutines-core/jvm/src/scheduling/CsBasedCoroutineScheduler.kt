/*
 * Copyright 2016-2023 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.scheduling

import kotlinx.atomicfu.*
import kotlinx.coroutines.internal.*
import kotlinx.coroutines.*
import kotlinx.coroutines.internal.ReentrantLock
import java.io.*
import java.lang.Runnable
import java.util.Stack
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.*
import java.util.concurrent.locks.LockSupport.*
import kotlin.concurrent.*
import kotlin.jvm.internal.*
import kotlin.random.*
import kotlin.math.*
import kotlin.random.Random

internal const val SCHED_DEBUG = false
internal const val LOCK_DEBUG = false

internal fun schedDebug(msg: String) {
    if (SCHED_DEBUG)
        System.err.println(msg)
}

internal fun lockDebug(msg: String) {
    if (LOCK_DEBUG)
        System.err.println(msg)
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
        private const val MIN_THREADS = CoroutineScheduler.MIN_SUPPORTED_POOL_SIZE
        private const val THREAD_TIMEOUT_MS = 20 * 1000L
        private const val DISPATCH_QUANTUM_MS = 30L

        private const val SHIFT_LENGTH = 16

        private const val SHIFT_PROCESSING_WORK =   0
        private const val SHIFT_EXISTING_THREADS =  SHIFT_LENGTH
        private const val SHIFT_THREADS_GOAL =      SHIFT_LENGTH * 2

        private const val MASK_PROCESSING_WORK =    (1L shl SHIFT_LENGTH) - 1
        private const val MASK_EXISTING_THREADS =   MASK_PROCESSING_WORK shl SHIFT_EXISTING_THREADS
        private const val MASK_THREADS_GOAL =       MASK_PROCESSING_WORK shl SHIFT_THREADS_GOAL
    }

    private val numProcessors = Runtime.getRuntime().availableProcessors()
    private val threadsToAddWithoutDelay = numProcessors
    private val threadsPerDelayStep = numProcessors
    private val threadCounts = AtomicLong(updateNumThreadsGoal(0L, corePoolSize))
    private val workerStack = Stack<Worker>()
    private val numRequestedWorkers = atomic(0)
    private val _isTerminated = atomic(false)
    private val hasOutstandingThreadRequest = atomic(0)
    private val gateThreadRunningState = atomic(0)
    private val lastDequeueTime = atomic(0L)
    private val runGateThreadEvent = AutoResetEvent(true)
    private val delayEvent = AutoResetEvent(false)
    private val delayHelper = DelayHelper()
    private val semaphore = Semaphore(0)
    private val threadAdjustmentLock = ReentrantLock()

    // TODO - decide if lower bound is corePoolSize or 1
    private val hillClimber = HillClimbing(corePoolSize, maxPoolSize)

    private var currentSampleStartTime = 0L
    private var threadAdjustmentIntervalMs = 0
    private var completionCount = 0
    private var priorCompletionCount = 0
    @Volatile private var nextCompletedWorkRequestsTime = 0L
    private var priorCompletedWorkRequestTime = 0L
    private var numBlockingTasks = 0
    private var numThreadsAddedDueToBlocking = 0
    private var createdWorkers = 0
    private var pendingBlockingAdjustment = PendingBlockingAdjustment.None

    @JvmField
    val workers = ResizableAtomicArray<CsBasedCoroutineScheduler.Worker>((corePoolSize + 1) * 2)

    @JvmField
    val globalQueue = GlobalQueue()

    val isTerminated: Boolean get() = _isTerminated.value

    private val minThreadsGoal: Int
        get() {
            return min(getNumThreadsGoal(threadCounts.get()), targetThreadsForBlockingAdjustment)
        }

    private val targetThreadsForBlockingAdjustment: Int
        get() {
            return if (numBlockingTasks <= 0) {
                corePoolSize
            } else {
                min(corePoolSize + numBlockingTasks, maxPoolSize)
            }
        }

    private fun getThreadCountsValue(data: Long, mask: Long, shift: Int): Int {
        return ((data and mask) shr shift).toInt()
    }

    private fun getThreadCountsUpdatedData(data: Long, value: Int, mask: Long, shift: Int): Long {
        return (data and mask.inv()) or (value.toLong() shl shift)
    }

    private fun getNumProcessingWork(data: Long = threadCounts.get()) =
        getThreadCountsValue(data, MASK_PROCESSING_WORK, SHIFT_PROCESSING_WORK)
    private fun getNumExistingThreads(data: Long = threadCounts.get()) =
        getThreadCountsValue(data, MASK_EXISTING_THREADS, SHIFT_EXISTING_THREADS)
    private fun getNumThreadsGoal(data: Long = threadCounts.get()) =
        getThreadCountsValue(data, MASK_THREADS_GOAL, SHIFT_THREADS_GOAL)

    // set functions only return updated value, don't change anything in place
    private fun updateNumProcessingWork(data: Long, value: Int) = getThreadCountsUpdatedData(data, value, MASK_PROCESSING_WORK, SHIFT_PROCESSING_WORK)
    private fun updateNumExistingThreads(data: Long, value: Int) = getThreadCountsUpdatedData(data, value, MASK_EXISTING_THREADS, SHIFT_EXISTING_THREADS)
    private fun updateNumThreadsGoal(data: Long, value: Int) = getThreadCountsUpdatedData(data, value, MASK_THREADS_GOAL, SHIFT_THREADS_GOAL)

    private fun decrementNumProcessingWork(data: Long) = data - (1L shl SHIFT_PROCESSING_WORK)
    private fun decrementNumExistingThreads(data: Long) = data - (1L shl SHIFT_EXISTING_THREADS)

    private fun interlockedSetNumThreadsGoal(value: Int): Long {
        var counts = threadCounts.get()
        while (true) {
            val newCounts = updateNumThreadsGoal(counts, value)
            val countsBeforeUpdate = threadCounts.compareAndExchange(counts, newCounts)
            if (countsBeforeUpdate == counts) {
                return newCounts
            }
            counts = countsBeforeUpdate
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

        delayEvent.set()
        runGateThreadEvent.set()
        semaphore.release(CoroutineScheduler.MAX_SUPPORTED_POOL_SIZE)

        val currentWorker = currentWorker()
        val created = synchronized(workers) { createdWorkers }

        for (i in 1..created) {
            val worker = workers[i]!!
            if (worker !== currentWorker) {
                while (worker.isAlive) {
                    LockSupport.unpark(worker)
                    worker.join(timeout)
                }
                worker.localQueue.offloadAllWorkTo(globalQueue)
            }
        }

        globalQueue.close()

        while (true) {
            val task = currentWorker?.localQueue?.poll()
                ?: globalQueue.removeFirstOrNull()
                ?: break
            runSafely(task)
        }
    }

    private fun runSafely(task: Task) {
        try {
            task.run()
        } catch (e: Throwable) {
            val thread = Thread.currentThread()
            thread.uncaughtExceptionHandler.uncaughtException(thread, e)
        } finally {
            unTrackTask()
        }
    }

    private fun enqueue(task: Task, tailDispatch: Boolean) {
        val currentWorker = currentWorker()
        val notAdded = currentWorker.submitToLocalQueue(task, tailDispatch)
        if (notAdded != null) {
            if (!addToGlobalQueue(notAdded)) {
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

    private fun markThreadRequestSatisfied() {
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
        // TODO - check for better solution
        // Scenario not working for now:
        // Many blocking tasks are created, many concurrent thread requests, some fail

//        if (hasOutstandingThreadRequest.compareAndSet(0, 1)) {
//            requestWorker()
//        }

        requestWorker()
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

    private fun notifyWorkItemComplete(currentTimeMs: Long): Boolean {
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
            if (!threadAdjustmentLock.tryLock()) {
                return
            }
            try {
                val counts = threadCounts.get()

                if (getNumProcessingWork(counts) > getNumThreadsGoal(counts) ||
                    pendingBlockingAdjustment != PendingBlockingAdjustment.None) {
                    return
                }

                val currentTicks = System.currentTimeMillis()
                val elapsedMs = currentTicks - currentSampleStartTime

                if (elapsedMs >= threadAdjustmentIntervalMs / 2) {
                    val numCompletions = completionCount - priorCompletionCount
                    val oldNumThreadsGoal = getNumThreadsGoal(counts)
                    val updateResult = hillClimber.update(oldNumThreadsGoal, elapsedMs / 1000.0, numCompletions)
                    val newNumThreadsGoal = updateResult.first
                    threadAdjustmentIntervalMs = updateResult.second

                    if (oldNumThreadsGoal != newNumThreadsGoal) {
                        interlockedSetNumThreadsGoal(newNumThreadsGoal)
                        if (newNumThreadsGoal > oldNumThreadsGoal) {
                            addWorker = true
                        }
                    }

                    priorCompletionCount = numCompletions
                    nextCompletedWorkRequestsTime = currentTicks + threadAdjustmentIntervalMs
                    priorCompletedWorkRequestTime = currentTicks
                    currentSampleStartTime = currentTicks
                }
            } finally {
                threadAdjustmentLock.unlock()
            }
        }
        if (addWorker) {
            maybeAddWorker()
        }
    }

    private fun shouldStopProcessingWorkNow(): Boolean {
        synchronized(this) {
            var counts = threadCounts.get()
            while (true) {
                if (getNumProcessingWork(counts) <= getNumThreadsGoal(counts)) {
                    return false
                }

                val newCounts = decrementNumProcessingWork(counts)
                val oldCounts = threadCounts.compareAndExchange(counts, newCounts)

                if (oldCounts == counts) {
                    return true
                }

                counts = oldCounts
                // TODO - remove this, doesn't compile with above line without it for some reason
                require(counts == oldCounts)
            }
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

            val counts = threadCounts.get()

            if (getNumProcessingWork(counts) > getNumThreadsGoal(counts)) {
                return false
            }

            return pendingBlockingAdjustment == PendingBlockingAdjustment.None
        }
    }

    private fun maybeAddWorker() {
        schedDebug("[$schedulerName] maybeAddWorker()")

        var counts = threadCounts.get()

        var numExistingThreads: Int
        var numProcessingWork: Int
        var newNumExistingThreads: Int
        var newNumProcessingWork: Int

        synchronized(this) {
            while (true) {
                numProcessingWork = getNumProcessingWork(counts)
                if (numProcessingWork >= getNumThreadsGoal(counts)) {
                    return
                }

                newNumProcessingWork = numProcessingWork + 1

                if (numBlockingTasks > 0) {
                    newNumProcessingWork = max(newNumProcessingWork, targetThreadsForBlockingAdjustment)
                }

                numExistingThreads = getNumExistingThreads(counts)
                newNumExistingThreads = max(numExistingThreads, newNumProcessingWork)

                // TODO - decide if it's desired
                if (newNumExistingThreads == numExistingThreads && newNumExistingThreads < corePoolSize) {
                    newNumExistingThreads++
                }

                var newCounts = counts

                newCounts = updateNumProcessingWork(newCounts, newNumProcessingWork)
                newCounts = updateNumExistingThreads(newCounts, newNumExistingThreads)

                val oldCounts = threadCounts.compareAndExchange(counts, newCounts)

                if (oldCounts == counts) {
                    break
                }

                counts = oldCounts
            }
        }

        val toCreate = newNumExistingThreads - numExistingThreads
        val toRelease = newNumProcessingWork - numProcessingWork

        if (toRelease > 0) {
            semaphore.release(toRelease)
        }

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
            var counts = threadCounts.get()
            while (true) {
                val newCounts = decrementNumProcessingWork(counts)
                val countsBeforeUpdate = threadCounts.compareAndExchange(counts, newCounts)
                if (countsBeforeUpdate == counts) {
                    break
                }
                counts = countsBeforeUpdate
            }
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
        // Compiler doesn't like val inside withLock
        var result = (0L to false)

        synchronized(this) {
            lockDebug("${Thread.currentThread().name} 3.1")
            threadAdjustmentLock.withLock {
                lockDebug("${Thread.currentThread().name} 3.2")
                result = performBlockingAdjustmentSync(previousDelayElapsed)
            }
            lockDebug("${Thread.currentThread().name} 3.3")
        }

        val nextDelayMs = result.first
        val addWorker = result.second

        if (addWorker) {
            maybeAddWorker()
        }

        return nextDelayMs
    }

    private fun performBlockingAdjustmentSync(previousDelayElapsed: Boolean): Pair<Long, Boolean> {
        var addWorker = false
        require(pendingBlockingAdjustment != PendingBlockingAdjustment.None)
        pendingBlockingAdjustment = PendingBlockingAdjustment.None
        val targetThreadsGoal = targetThreadsForBlockingAdjustment

        var counts = threadCounts.get()
        var numThreadsGoal = getNumThreadsGoal(counts)

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
            interlockedSetNumThreadsGoal(numThreadsGoal)
            hillClimber.forceChange(numThreadsGoal, HillClimbing.StateOrTransition.CooperativeBlocking)

            return (0L to false)
        }

        // TODO - decide corePoolSize or MIN_THREADS
        val configuredMaxThreadsWithoutDelay = min(MIN_THREADS + threadsToAddWithoutDelay, maxPoolSize)

        do {
            val maxThreadsGoalWithoutDelay = max(configuredMaxThreadsWithoutDelay, min(getNumExistingThreads(counts), maxPoolSize))
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
            counts = interlockedSetNumThreadsGoal(newNumThreadsGoal)
            hillClimber.forceChange(newNumThreadsGoal, HillClimbing.StateOrTransition.CooperativeBlocking)

            if (getNumProcessingWork(counts) >= numThreadsGoal && numRequestedWorkers.value > 0) {
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

    private fun wakeGateThread() {
        delayEvent.set()
        ensureGateThreadRunning()
    }

    private fun sufficientDelaySinceLastDequeue(): Boolean {
        val delay = System.currentTimeMillis() - lastDequeueTime.value

        // TODO - check for CPU utilization
        val minimumDelay = GATE_ACTIVITIES_PERIOD_MS

        return delay > minimumDelay
    }

    private fun notifyThreadBlocked() {
        var shouldWakeGateThread = false
        synchronized(this) {
            lockDebug("${Thread.currentThread().name} 4.1")
            threadAdjustmentLock.withLock {
                lockDebug("${Thread.currentThread().name} 4.2")
                numBlockingTasks++
                require(numBlockingTasks > 0)

                if (pendingBlockingAdjustment != PendingBlockingAdjustment.WithDelayIfNecessary &&
                    getNumThreadsGoal() < targetThreadsForBlockingAdjustment) {

                    if (pendingBlockingAdjustment == PendingBlockingAdjustment.None) {
                        shouldWakeGateThread = true
                    }

                    pendingBlockingAdjustment = PendingBlockingAdjustment.WithDelayIfNecessary
                }
            }
            lockDebug("${Thread.currentThread().name} 4.3")
        }

        if (shouldWakeGateThread) {
            wakeGateThread()
        }
    }

    private fun notifyThreadUnblocked() {
        var shouldWakeGateThread = false
        synchronized(this) {
            lockDebug("${Thread.currentThread().name} 5.1")
            threadAdjustmentLock.withLock {
                lockDebug("${Thread.currentThread().name} 5.2")
                numBlockingTasks--
                if (pendingBlockingAdjustment != PendingBlockingAdjustment.Immediately &&
                    numThreadsAddedDueToBlocking > 0 &&
                    getNumThreadsGoal() > targetThreadsForBlockingAdjustment) {

                    shouldWakeGateThread = true
                    pendingBlockingAdjustment = PendingBlockingAdjustment.Immediately
                }
            }
            lockDebug("${Thread.currentThread().name} 5.3")
        }

        if (shouldWakeGateThread) {
            wakeGateThread()
        }
    }

    private fun beforeTask(taskMode: Int) {
        if (taskMode == TASK_PROBABLY_BLOCKING) {
            notifyThreadBlocked()
        }
    }

    private fun afterTask(taskMode: Int) {
        if (taskMode == TASK_PROBABLY_BLOCKING) {
            notifyThreadUnblocked()
        }
    }

    internal inner class Worker private constructor() : Thread() {
        init {
            isDaemon = true
        }

        private var indexInArray = 0
            set(index) {
                name = "$schedulerName-worker-${if (index == 0) "TERMINATED" else index.toString()}"
                field = index
            }

        constructor(index: Int): this() {
            indexInArray = index
        }

        @JvmField
        val localQueue: WorkQueue = WorkQueue()

        val inStack = atomic(false)

        private val stolenTask: Ref.ObjectRef<Task?> = Ref.ObjectRef()
        private var rngState = Random.nextInt()

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
                lockDebug("${Thread.currentThread().name} 1.1")
                threadAdjustmentLock.withLock {
                    lockDebug("${Thread.currentThread().name} 1.2")
                    var counts = threadCounts.get()
                    while (true) {
                        if (getNumExistingThreads(counts) <= getNumProcessingWork(counts)) {
                            lockDebug("${Thread.currentThread().name} 1.3")
                            return false
                        }

                        var newCounts = decrementNumExistingThreads(counts)
                        val newNumExistingThreads = getNumExistingThreads(newCounts)
                        val newNumThreadsGoal = max(minThreadsGoal, min(newNumExistingThreads, getNumThreadsGoal(counts)))

                        newCounts = updateNumThreadsGoal(newCounts, newNumThreadsGoal)
                        val oldCounts = threadCounts.compareAndExchange(counts, newCounts)

                        if (oldCounts == counts) {
                            hillClimber.forceChange(newNumThreadsGoal, HillClimbing.StateOrTransition.ThreadTimedOut)
                            lockDebug("${Thread.currentThread().name} 1.3")
                            return true
                        }

                        counts = oldCounts
                        // TODO - delete, again wouldn't compile
                        require(counts == oldCounts)
                    }
                }
            }

            // TODO - delete, unreachable
            return false
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

        private fun nextInt(upperBound: Int): Int {
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
            markThreadRequestSatisfied()

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

        // TODO - maybe rewrite
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
                            lockDebug("${Thread.currentThread().name} 2.1")
                            threadAdjustmentLock.withLock {
                                lockDebug("${Thread.currentThread().name} 2.2")
                                var counts = threadCounts.get()
                                while (getNumProcessingWork(counts) < maxPoolSize
                                    && getNumProcessingWork(counts) >= getNumThreadsGoal(counts)) {

                                    val newNumThreadsGoal = getNumProcessingWork(counts) + 1
                                    val newCounts = updateNumThreadsGoal(counts, newNumThreadsGoal)

                                    val countsBeforeUpdate = threadCounts.compareAndExchange(counts, newCounts)
                                    if (countsBeforeUpdate == counts) {
                                        hillClimber.forceChange(newNumThreadsGoal, HillClimbing.StateOrTransition.Starvation)
                                        addWorker = true
                                        break
                                    }

                                    counts = countsBeforeUpdate
                                }
                            }
                            lockDebug("${Thread.currentThread().name} 2.3")
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

    private enum class PendingBlockingAdjustment {
        None,
        Immediately,
        WithDelayIfNecessary
    }
}