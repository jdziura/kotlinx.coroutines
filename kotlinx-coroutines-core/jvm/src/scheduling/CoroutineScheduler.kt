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
import java.util.concurrent.locks.LockSupport
import kotlin.concurrent.*
import kotlin.jvm.internal.*
import kotlin.math.*
import kotlin.random.Random

// TODO - support idleWorkerKeepAliveNs (for now workers actually terminate only on shutdown
// TODO - decide if lower bound in HillClimber number of threads should be 1 or corePoolSize
internal class CoroutineScheduler(
    @JvmField val corePoolSize: Int,
    @JvmField val maxPoolSize: Int,
    @JvmField val idleWorkerKeepAliveNs: Long = IDLE_WORKER_KEEP_ALIVE_NS,
    @JvmField val schedulerName: String = DEFAULT_SCHEDULER_NAME
) : Executor, Closeable {
    init {
        require(corePoolSize >= MIN_SUPPORTED_POOL_SIZE) {
            "Core pool size $corePoolSize should be at least $MIN_SUPPORTED_POOL_SIZE"
        }
        require(maxPoolSize >= corePoolSize) {
            "Max pool size $maxPoolSize should be greater than or equals to core pool size $corePoolSize"
        }
        require(maxPoolSize <= MAX_SUPPORTED_POOL_SIZE) {
            "Max pool size $maxPoolSize should not exceed maximal supported number of threads $MAX_SUPPORTED_POOL_SIZE"
        }
        require(idleWorkerKeepAliveNs > 0) {
            "Idle worker keep alive time $idleWorkerKeepAliveNs must be positive"
        }
    }

    companion object {
        @JvmField
        val NOT_IN_STACK = Symbol("NOT_IN_STACK")

        private const val MAX_RUNS = 2
        private const val GATE_THREAD_RUNNING_MASK = 0x4
        private const val GATE_ACTIVITIES_PERIOD_MS = DelayHelper.GATE_ACTIVITIES_PERIOD_MS
        private const val DELAY_STEP_MS = 25L
        private const val MAX_DELAY_MS = 250L
        private const val DISPATCH_QUANTUM_MS = 30L

        private const val PARKED = -1
        private const val CLAIMED = 0
        private const val TERMINATED = 1

        private const val SHIFT_LENGTH = 16

        private const val SHIFT_PROCESSING_WORK =   0
        private const val SHIFT_EXISTING_THREADS =  SHIFT_LENGTH
        private const val SHIFT_THREADS_GOAL =      SHIFT_LENGTH * 2

        private const val MASK_PROCESSING_WORK =    (1L shl SHIFT_LENGTH) - 1
        private const val MASK_EXISTING_THREADS =   MASK_PROCESSING_WORK shl SHIFT_EXISTING_THREADS
        private const val MASK_THREADS_GOAL =       MASK_PROCESSING_WORK shl SHIFT_THREADS_GOAL

        private const val PARKED_INDEX_MASK = MASK_PROCESSING_WORK
        private const val PARKED_VERSION_MASK = MASK_PROCESSING_WORK.inv()
        private const val PARKED_VERSION_INC = 1L shl SHIFT_LENGTH

        internal const val MIN_SUPPORTED_POOL_SIZE = 1
        internal const val MAX_SUPPORTED_POOL_SIZE = (1 shl SHIFT_LENGTH) - 2
    }

    private val threadsToAddWithoutDelay = AVAILABLE_PROCESSORS
    private val threadsPerDelayStep = AVAILABLE_PROCESSORS
    private val threadCounts = AtomicLong(updateNumThreadsGoal(0L, corePoolSize))
    private val workerPermits = atomic(0)
    private val numRequestedWorkers = atomic(0)
    private val _isTerminated = atomic(false)
    private val hasOutstandingThreadRequest = atomic(0)
    private val gateThreadRunningState = atomic(0)
    private val completionCount = atomic(0)
    private val numBlockingTasks = atomic(0)
    private val createdWorkers = atomic(0)
    private val lastDequeueTime = atomic(0L)
    private val parkedWorkersStack = atomic(0L)
    private val runGateThreadEvent = AutoResetEvent(true)
    private val delayEvent = AutoResetEvent(false)
    private val delayHelper = DelayHelper()
    private val threadAdjustmentLock = ReentrantLock()

    private val semaphore = Semaphore(0)

    private val hillClimber = HillClimbing(MIN_SUPPORTED_POOL_SIZE, maxPoolSize)

    @Volatile private var nextCompletedWorkRequestsTime = 0L
    @Volatile private var priorCompletedWorkRequestTime = 0L
    @Volatile private var pendingBlockingAdjustment = PendingBlockingAdjustment.None

    private var currentSampleStartTime = 0L
    private var threadAdjustmentIntervalMs = 0
    private var priorCompletionCount = 0
    private var numThreadsAddedDueToBlocking = 0

    @JvmField
    val workers = ResizableAtomicArray<CoroutineScheduler.Worker>((corePoolSize + 1) * 2)

    @JvmField
    val globalQueue = GlobalQueue()

    val isTerminated: Boolean get() = _isTerminated.value

    private val minThreadsGoal: Int
        get() {
            return min(getNumThreadsGoal(threadCounts.get()), targetThreadsForBlockingAdjustment)
        }

    private val targetThreadsForBlockingAdjustment: Int
        get() {
            val numBlocking: Int = numBlockingTasks.value
            return if (numBlocking <= 0) {
                corePoolSize
            } else {
                min(corePoolSize + numBlocking, maxPoolSize)
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

    fun parkedWorkersStackTopUpdate(worker: Worker, oldIndex: Int, newIndex: Int) {
        parkedWorkersStack.loop { top ->
            val index = (top and PARKED_INDEX_MASK).toInt()
            val updVersion = (top + PARKED_VERSION_INC) and PARKED_VERSION_MASK
            val updIndex = if (index == oldIndex) {
                if (newIndex == 0) {
                    parkedWorkersStackNextIndex(worker)
                } else {
                    newIndex
                }
            } else {
                index
            }
            if (updIndex < 0) return@loop
            if (parkedWorkersStack.compareAndSet(top, updVersion or updIndex.toLong())) return
        }
    }

    fun parkedWorkersStackPush(worker: Worker): Boolean {
        if (worker.nextParkedWorker !== NOT_IN_STACK) return false

        parkedWorkersStack.loop { top ->
            val index = (top and PARKED_INDEX_MASK).toInt()
            val updVersion = (top + PARKED_VERSION_INC) and PARKED_VERSION_MASK
            val updIndex = worker.indexInArray
            assert { updIndex != 0 }
            worker.nextParkedWorker = workers[index]
            if (parkedWorkersStack.compareAndSet(top, updVersion or updIndex.toLong())) return true
        }
    }

    private fun parkedWorkersStackPop(): Worker? {
        parkedWorkersStack.loop { top ->
            val index = (top and PARKED_INDEX_MASK).toInt()
            val worker = workers[index] ?: return null // stack is empty
            val updVersion = (top + PARKED_VERSION_INC) and PARKED_VERSION_MASK
            val updIndex = parkedWorkersStackNextIndex(worker)
            if (updIndex < 0) return@loop // retry
            if (parkedWorkersStack.compareAndSet(top, updVersion or updIndex.toLong())) {
                worker.nextParkedWorker = NOT_IN_STACK
                return worker
            }
        }
    }

    private fun parkedWorkersStackNextIndex(worker: Worker): Int {
        var next = worker.nextParkedWorker
        findNext@ while (true) {
            when {
                next === NOT_IN_STACK -> return -1 // we are too late -- other thread popped this element, retry
                next === null -> return 0 // stack becomes empty
                else -> {
                    val nextWorker = next as Worker
                    val updIndex = nextWorker.indexInArray
                    if (updIndex != 0) return updIndex // found good index for next worker
                    next = nextWorker.nextParkedWorker
                }
            }
        }
    }







    fun dispatch(block: Runnable, taskContext: TaskContext = NonBlockingContext, tailDispatch: Boolean = false) {
        trackTask()
        val task = createTask(block, taskContext)
        enqueue(task, tailDispatch)
    }

    fun createTask(block: Runnable, taskContext: TaskContext): Task {
        val nanoTime = schedulerTimeSource.nanoTime()
        if (block is Task) {
            block.submissionTime = nanoTime
            block.taskContext = taskContext
            return block
        }
        return TaskImpl(block, nanoTime, taskContext)
    }

    override fun execute(command: Runnable) = dispatch(command)

    override fun close() = shutdown(10_000L)

    fun shutdown(timeout: Long) {
        if (!_isTerminated.compareAndSet(false, true)) return

        delayEvent.set()
        runGateThreadEvent.set()
        releasePermits(MAX_SUPPORTED_POOL_SIZE)

        val currentWorker = currentWorker()
        val created = synchronized(workers) { createdWorkers.value }

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

//        while (true) {
//            val task = currentWorker?.localQueue?.poll()
//                ?: globalQueue.removeFirstOrNull()
//                ?: break
//            runSafely(task)
//        }
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

    private fun markThreadRequestSatisfied() {
        hasOutstandingThreadRequest.getAndSet(0)
    }

    private fun Worker?.submitToLocalQueue(task: Task, tailDispatch: Boolean): Task? {
        if (this == null) return task
        if (task.isBlocking) return task
        if (isTerminated) return task
        mayHaveLocalTasks = true
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
        while (true) {
            val worker = parkedWorkersStackPop() ?: return false
            if (worker.workerCtl.compareAndSet(PARKED, CLAIMED)) {
                LockSupport.unpark(worker)
                return true
            }
        }
    }

    private fun requestWorker() {
        numRequestedWorkers.incrementAndGet()
        maybeAddWorker()
        ensureGateThreadRunning()
    }

    private fun notifyWorkItemComplete(currentTimeMs: Long): Boolean {
        notifyWorkItemProgress(currentTimeMs)
        return !shouldStopProcessingWorkNow()
    }

    private fun notifyWorkItemProgress(currentTimeMs: Long) {
        completionCount.incrementAndGet()
        lastDequeueTime.getAndSet(currentTimeMs)
        if (shouldAdjustMaxWorkersActive(currentTimeMs)) {
            adjustMaxWorkersActive()
        }
    }

    private fun adjustMaxWorkersActive() {
        var addWorker = false
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
                val numCompletions = completionCount.value - priorCompletionCount
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
        if (addWorker) {
            maybeAddWorker()
        }
    }

    private fun shouldStopProcessingWorkNow(): Boolean {
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

    private fun shouldAdjustMaxWorkersActive(currentTimeMs: Long): Boolean {
        if (!HillClimbing.ENABLED) {
            return false
        }

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

    private fun maybeAddWorker() {
        var counts = threadCounts.get()

        var numExistingThreads: Int
        var numProcessingWork: Int
        var newNumExistingThreads: Int
        var newNumProcessingWork: Int

        while (true) {
            numProcessingWork = getNumProcessingWork(counts)
            if (numProcessingWork >= getNumThreadsGoal(counts)) {
                return
            }

            newNumProcessingWork = numProcessingWork + 1

//            newNumProcessingWork = max(newNumProcessingWork, getNumThreadsGoal(counts))

            if (numBlockingTasks.value > 0) {
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

        val toCreate = newNumExistingThreads - numExistingThreads
        val toRelease = newNumProcessingWork - numProcessingWork

        if (toRelease > 0) {
            releasePermits(toRelease)
        }

        repeat(toCreate) {
            createWorker()
        }
    }

    private fun createWorker() {
        val worker: Worker
        synchronized(workers) {
            if (isTerminated) return
            val newIndex = createdWorkers.incrementAndGet()
            require(newIndex > 0 && workers[newIndex] == null)
            worker = Worker(newIndex)
            workers.setSynchronized(newIndex, worker)
        }
        worker.start()
    }

    private fun removeWorkingWorker() {
        var counts = threadCounts.get()
        while (true) {
            val newCounts = decrementNumProcessingWork(counts)
            val countsBeforeUpdate = threadCounts.compareAndExchange(counts, newCounts)
            if (countsBeforeUpdate == counts) {
                break
            }
            counts = countsBeforeUpdate
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

        threadAdjustmentLock.withLock {
            result = performBlockingAdjustmentSync(previousDelayElapsed)
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

        // TODO - decide corePoolSize or MIN_SUPPORTED_POOL_SIZE
        val configuredMaxThreadsWithoutDelay = min(MIN_SUPPORTED_POOL_SIZE + threadsToAddWithoutDelay, maxPoolSize)

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
        numBlockingTasks.incrementAndGet()
        threadAdjustmentLock.withLock {
            require(numBlockingTasks.value > 0)

            if (pendingBlockingAdjustment != PendingBlockingAdjustment.WithDelayIfNecessary &&
                getNumThreadsGoal() < targetThreadsForBlockingAdjustment) {

                if (pendingBlockingAdjustment == PendingBlockingAdjustment.None) {
                    shouldWakeGateThread = true
                }

                pendingBlockingAdjustment = PendingBlockingAdjustment.WithDelayIfNecessary
            }
        }

        if (shouldWakeGateThread) {
            wakeGateThread()
        }
    }

    private fun notifyThreadUnblocked() {
        var shouldWakeGateThread = false
        numBlockingTasks.decrementAndGet()
        threadAdjustmentLock.withLock {
            if (pendingBlockingAdjustment != PendingBlockingAdjustment.Immediately &&
                numThreadsAddedDueToBlocking > 0 &&
                getNumThreadsGoal() > targetThreadsForBlockingAdjustment) {

                shouldWakeGateThread = true
                pendingBlockingAdjustment = PendingBlockingAdjustment.Immediately
            }
        }

        if (shouldWakeGateThread) {
            wakeGateThread()
        }
    }

    private fun releasePermits(permits: Int) {
        workerPermits.addAndGet(permits)
        for (i in 0 until permits) {
            if (!tryUnpark()) return
        }
//        semaphore.release(permits)
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

        var indexInArray = 0
            set(index) {
                name = "$schedulerName-worker-${if (index == 0) "TERMINATED" else index.toString()}"
                field = index
            }

        constructor(index: Int): this() {
            indexInArray = index
        }

        @JvmField
        val localQueue: WorkQueue = WorkQueue()

        @JvmField
        var mayHaveLocalTasks = false

        @Volatile
        var nextParkedWorker: Any? = NOT_IN_STACK

        val workerCtl = atomic(CLAIMED)

        private val stolenTask: Ref.ObjectRef<Task?> = Ref.ObjectRef()
        private var rngState = Random.nextInt()
        private var terminationDeadline = 0L
        private var acquireTimedOut = false

        inline val scheduler get() = this@CoroutineScheduler

        override fun run() = runWorker()

        private fun runWorker() {
            while (!isTerminated) {
                while (!isTerminated) {
                    if (tryAcquirePermit()) {
                        doWork()
                    } else {
                        break
                    }
                }
                if (shouldExitWorker()) {
                    tryTerminateWorker()
                    break
                }
            }
        }

        private fun doWork() {
            var alreadyRemovedWorkingWorker = false
            while (takeActiveRequest()) {
                lastDequeueTime.getAndSet(System.currentTimeMillis())
                if (!dispatchFromQueue()) {
                    alreadyRemovedWorkingWorker = true
                    break
                } else {
                    mayHaveLocalTasks = false
                }

                if (numRequestedWorkers.value <= 0) {
                    break
                }

//                yield()
            }

            if (!alreadyRemovedWorkingWorker) {
                removeWorkingWorker()
            }
        }

        private fun tryAcquirePermit(): Boolean {
            while (!acquireTimedOut) {
                var cnt = workerPermits.value
                while (cnt > 0) {
                    if (workerPermits.compareAndSet(cnt, cnt - 1)) {
                        acquireTimedOut = false
                        return true
                    }
                    cnt = workerPermits.value
                }
                tryPark()
            }
            acquireTimedOut = false
            return false
//            return semaphore.tryAcquire(idleWorkerKeepAliveNs, TimeUnit.NANOSECONDS)
        }

        private fun findTask(scanLocalQueue: Boolean): Task? {
            if (scanLocalQueue) {
                val globalFirst = nextInt(2 * corePoolSize) == 0
                if (globalFirst) pollGlobalQueues()?.let { return it }
                localQueue.poll()?.let { return it }
                if (!globalFirst) pollGlobalQueues()?.let { return it }
            } else {
                pollGlobalQueues()?.let { return it }
            }

            val (task, missedSteal) = trySteal(STEAL_ANY)
            if (missedSteal) {
                ensureThreadRequested()
            }
            return task
        }

        private fun shouldExitWorker(): Boolean {
            threadAdjustmentLock.withLock {
                var counts = threadCounts.get()
                while (true) {
                    if (getNumExistingThreads(counts) <= getNumProcessingWork(counts)) {
                        return false
                    }

                    var newCounts = decrementNumExistingThreads(counts)
                    val newNumExistingThreads = getNumExistingThreads(newCounts)
                    val newNumThreadsGoal = max(minThreadsGoal, min(newNumExistingThreads, getNumThreadsGoal(counts)))

                    newCounts = updateNumThreadsGoal(newCounts, newNumThreadsGoal)
                    val oldCounts = threadCounts.compareAndExchange(counts, newCounts)

                    if (oldCounts == counts) {
                        hillClimber.forceChange(newNumThreadsGoal, HillClimbing.StateOrTransition.ThreadTimedOut)
                        return true
                    }

                    counts = oldCounts
                    // TODO - delete, again wouldn't compile
                    require(counts == oldCounts)
                }
            }

            // TODO - delete, unreachable
            return false
        }

        private fun tryPark() {
            if (!inStack()) {
                parkedWorkersStackPush(this)
                return
            }
            workerCtl.value = PARKED

            while (inStack() && workerCtl.value == PARKED) {
                if (isTerminated || workerCtl.value == TERMINATED) break
                interrupted()
                park()
            }
        }

        private fun inStack(): Boolean = nextParkedWorker !== NOT_IN_STACK

        private fun park() {
            if (terminationDeadline == 0L) terminationDeadline = System.nanoTime() + idleWorkerKeepAliveNs
            LockSupport.parkNanos(idleWorkerKeepAliveNs)
            if (System.nanoTime() - terminationDeadline >= 0) {
                terminationDeadline = 0L
                acquireTimedOut = true
            }
        }

        private fun tryTerminateWorker() {
            synchronized(workers) {
                if (isTerminated) return
                if (createdWorkers.value <= corePoolSize)
                if (!workerCtl.compareAndSet(PARKED, TERMINATED)) return
                val oldIndex = indexInArray
                indexInArray = 0

                parkedWorkersStackTopUpdate(this, oldIndex, 0)

                val lastIndex = createdWorkers.getAndDecrement()
                if (lastIndex != oldIndex) {
                    val lastWorker = workers[lastIndex]!!
                    workers.setSynchronized(oldIndex, lastWorker)
                    lastWorker.indexInArray = oldIndex
                    parkedWorkersStackTopUpdate(lastWorker, lastIndex, oldIndex)
                }

                workers.setSynchronized(lastIndex, null)
            }
        }

        private fun idleReset() {
            terminationDeadline = 0L
            acquireTimedOut = false
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
            markThreadRequestSatisfied()

            var workItem: Task? = findTask(mayHaveLocalTasks) ?: return true
            var startTickCount = System.currentTimeMillis()

            while (true) {
                if (workItem == null) {
                    workItem = findTask(mayHaveLocalTasks) ?: return true
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
            val created: Int = createdWorkers.value

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

        private fun executeWorkItem(workItem: Task) {
            idleReset()
            beforeTask(workItem.mode)
            runSafely(workItem)
            afterTask(workItem.mode)
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
                        threadAdjustmentLock.withLock {
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

/**
 * Checks if the thread is part of a thread pool that supports coroutines.
 * This function is needed for integration with BlockHound.
 */
@JvmName("isSchedulerWorker")
internal fun isSchedulerWorker(thread: Thread) = thread is CoroutineScheduler.Worker

/**
 * Checks if the thread is running a CPU-bound task.
 * This function is needed for integration with BlockHound.
 */

// TODO - add support
@JvmName("mayNotBlock")
internal fun mayNotBlock(thread: Thread) = thread is CoroutineScheduler.Worker
//internal fun mayNotBlock(thread: Thread) = thread is CoroutineScheduler.Worker &&
//    thread.state == CoroutineScheduler.WorkerState.CPU_ACQUIRED