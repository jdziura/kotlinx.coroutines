/*
 * Copyright 2016-2023 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.scheduling

import kotlinx.atomicfu.*
import kotlinx.coroutines.internal.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.internal.ReentrantLock
import java.io.*
import java.lang.Runnable
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.LockSupport
import kotlin.jvm.internal.Ref.ObjectRef
import kotlin.math.*
import kotlin.random.Random

// If enabled, scheduler will dynamically adjust the number of active worker threads based on
// current throughput, trying to maximize it with minimum number of workers.
internal const val ENABLE_HILL_CLIMBING = true

// Starvation detection is performed by GateThread twice per second, and tries to inject a thread
// if no work has been done for this time.
internal const val ENABLE_STARVATION_DETECTION = true

// If enabled, tasks added to local queues will have to wait for a small period of time until they
// can be stealable. Does nothing if not using kotlin local queues.
internal const val ENABLE_MIN_DELAY_UNTIL_STEALING = false

// If set to true, there could be up to CPU-count active thread requests (entering ensureThreadRequested()).
// Otherwise, only 1 request is allowed at a time.
internal const val ENABLE_CONCURRENT_THREAD_REQUESTS = false

// If false, will use ported .NET implementation
internal const val USE_KOTLIN_LOCAL_QUEUES = false

// If false, will use JAVA ConcurrentLinkedQueue
internal const val USE_KOTLIN_GLOBAL_QUEUE = false

// If true, uses simple spinlock for places where steal/pop from local queues could interfere.
// Otherwise, uses reentrant lock
internal const val USE_DOTNET_QUEUE_SPINLOCK = true

// If true, uses custom .NET semaphore for managing number of active threads.
// It uses JAVA semaphore under the hood but wraps it with larger logic of spin waiting.
internal const val USE_DOTNET_SEMAPHORE = false

internal const val LOG_MAJOR_HC_ADJUSTMENTS = false

// [TODO] Clean time management in (should)adjustMaxWorkersActive()

@Suppress("NOTHING_TO_INLINE")
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
        private const val MAX_RUNS = 2
        private const val GATE_THREAD_RUNNING_MASK = 0x4
        private const val GATE_ACTIVITIES_PERIOD_MS = DelayHelper.GATE_ACTIVITIES_PERIOD_MS
        private const val DELAY_STEP_MS = 25L
        private const val MAX_DELAY_MS = 250L
        private const val PROCESS_BATCH_QUANTUM_MS = 30L

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

        // Used only for .NET semaphore
        private const val SEMAPHORE_SPIN_COUNT = 70

        internal const val MIN_SUPPORTED_POOL_SIZE = 1
        internal const val MAX_SUPPORTED_POOL_SIZE = (1 shl SHIFT_LENGTH) - 2
    }

    private val threadsToAddWithoutDelay = AVAILABLE_PROCESSORS
    private val threadsPerDelayStep = AVAILABLE_PROCESSORS
    private val threadCounts = AtomicLong(updateNumThreadsGoal(0L, corePoolSize))
    private val numOutstandingThreadRequests = AtomicInteger(0)
    private val numRequestedWorkers = atomic(0)
    private val _isTerminated = atomic(false)
    private val gateThreadRunningState = atomic(0)
    private val completionCount = atomic(0)
    private val createdWorkers = atomic(0)
    private val runGateThreadEvent = AutoResetEvent(true)
    private val delayEvent = AutoResetEvent(false)
    private val delayHelper = DelayHelper()
    private val threadAdjustmentLock = ReentrantLock()

    private val semaphore = Semaphore(0)
    private val semaphoreDotnet = LowLevelLifoSemaphore(0, SEMAPHORE_SPIN_COUNT)

    private val hillClimber = HillClimbing(this)

    @Volatile private var lastDequeueTime = 0L
    @Volatile private var nextCompletedWorkRequestsTime = 0L
    @Volatile private var pendingBlockingAdjustment = PendingBlockingAdjustment.None

    private var currentSampleStartTime = 0L
    private var threadAdjustmentIntervalMs = 0
    private var priorCompletionCount = 0
    private var numThreadsAddedDueToBlocking = 0

    // Number of blocking tasks that are currently being executed by workers.
    private var numBlockingTasks = 0

    @JvmField
    val workers = ResizableAtomicArray<CoroutineScheduler.Worker>((corePoolSize + 1) * 2)

    @JvmField
    val globalQueue = GlobalQueue()

    @JvmField
    val globalQueueJava = ConcurrentLinkedQueue<Task>()

    @JvmField
    val workStealingQueueList = WorkStealingQueueList()

    val isTerminated: Boolean inline get() = _isTerminated.value

    // Use only when threadAdjustmentLock is held.
    val minThreadsGoal: Int
        inline get() {
            return min(getNumThreadsGoal(), targetThreadsForBlockingAdjustment)
        }

    // Returns required number of threads to run, taking into account number of blocking tasks being currently executed.
    // Needs to be used only when threadAdjustmentLock is held.
    private val targetThreadsForBlockingAdjustment: Int
        inline get() {
            return if (numBlockingTasks <= 0) {
                corePoolSize
            } else {
                min(corePoolSize + numBlockingTasks, maxPoolSize)
            }
        }

    // Helper function to retrieve value from state.
    private inline fun getThreadCountsValue(data: Long, mask: Long, shift: Int): Int {
        return ((data and mask) shr shift).toInt()
    }

    // Helper function that returns a new state with updated value.
    private inline fun getThreadCountsUpdatedData(data: Long, value: Int, mask: Long, shift: Int): Long {
        return (data and mask.inv()) or (value.toLong() shl shift)
    }
    private inline fun getNumProcessingWork(data: Long = threadCounts.get()) =
        getThreadCountsValue(data, MASK_PROCESSING_WORK, SHIFT_PROCESSING_WORK)
    private inline fun getNumExistingThreads(data: Long = threadCounts.get()) =
        getThreadCountsValue(data, MASK_EXISTING_THREADS, SHIFT_EXISTING_THREADS)
    private inline fun getNumThreadsGoal(data: Long = threadCounts.get()) =
        getThreadCountsValue(data, MASK_THREADS_GOAL, SHIFT_THREADS_GOAL)
    private inline fun updateNumProcessingWork(data: Long, value: Int) =
        getThreadCountsUpdatedData(data, value, MASK_PROCESSING_WORK, SHIFT_PROCESSING_WORK)
    private inline fun updateNumExistingThreads(data: Long, value: Int) =
        getThreadCountsUpdatedData(data, value, MASK_EXISTING_THREADS, SHIFT_EXISTING_THREADS)
    private inline fun updateNumThreadsGoal(data: Long, value: Int) =
        getThreadCountsUpdatedData(data, value, MASK_THREADS_GOAL, SHIFT_THREADS_GOAL)
    private inline fun decrementNumProcessingWork(data: Long) = data - (1L shl SHIFT_PROCESSING_WORK)
    private inline fun decrementNumExistingThreads(data: Long) = data - (1L shl SHIFT_EXISTING_THREADS)

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

    fun dispatch(block: Runnable, taskContext: TaskContext = NonBlockingContext, tailDispatch: Boolean = false) {
        trackTask()
        val task = createTask(block, taskContext)
        val currentWorker = currentWorker()
        val notAdded = currentWorker.submitToLocalQueue(task, tailDispatch)
        if (notAdded != null) {
            if (!addToGlobalQueue(notAdded)) {
                throw RejectedExecutionException("$schedulerName was terminated")
            }
        }

        ensureThreadRequested()
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
                    if (ENABLE_MIN_DELAY_UNTIL_STEALING) {
                        LockSupport.unpark(worker)
                    }
                    worker.join(timeout)
                }
            }
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

    private fun Worker?.submitToLocalQueue(task: Task, tailDispatch: Boolean): Task? {
        if (this == null) return task
        if (task.isBlocking) return task
        if (isTerminated) return task

        return if (USE_KOTLIN_LOCAL_QUEUES) {
            localQueue.add(task, fair = tailDispatch)
        } else {
            localQueueDotnet.localPush(task)
            null
        }
    }

    private fun addToGlobalQueue(task: Task): Boolean {
        return if (USE_KOTLIN_GLOBAL_QUEUE) {
            globalQueue.addLast(task)
        } else {
            globalQueueJava.add(task)
            true
        }
    }

    // Requests for a thread to process work. Restricts number of concurrent request, to avoid over-parallelization.
    private fun ensureThreadRequested() {
        if (ENABLE_CONCURRENT_THREAD_REQUESTS) {
            // Restricts the number of requests to number of available processors at a time.
            var count = numOutstandingThreadRequests.value
            while (count < AVAILABLE_PROCESSORS) {
                val prev = numOutstandingThreadRequests.compareAndExchange(count, count + 1)
                if (prev == count) {
                    requestWorker()
                    break
                }
                count = prev
            }
        } else {
            // Restricts the number of requests to 1 at a time.
            if (numOutstandingThreadRequests.compareAndSet(0, 1)) {
                requestWorker()
            }
        }
    }

    // Notifies that a thread request has been satisfied, and allows another requests to be processed.
    private fun markThreadRequestSatisfied() {
        if (ENABLE_CONCURRENT_THREAD_REQUESTS) {
            var count = numOutstandingThreadRequests.value
            while (count > 0) {
                val prev = numOutstandingThreadRequests.compareAndExchange(count, count - 1)
                if (prev == count) {
                    break
                }
                count = prev
            }
        } else {
            numOutstandingThreadRequests.set(0)
        }

    }

    private fun currentWorker(): Worker? = (Thread.currentThread() as? Worker)?.takeIf { it.scheduler == this }

    private fun requestWorker() {
        numRequestedWorkers.incrementAndGet()
        maybeAddWorker()
        ensureGateThreadRunning()
    }

    // Calls hill climbing algorithm to potentially adjust number of workers.
    private fun adjustMaxWorkersActive() {
        if (!threadAdjustmentLock.tryLock()) {
            // The lock is held by someone else, they will take care of this for us.
            return
        }

        var addWorker = false

        try {
            val counts = threadCounts.get()
            if (getNumProcessingWork(counts) > getNumThreadsGoal(counts) ||
                pendingBlockingAdjustment != PendingBlockingAdjustment.None) {
                return
            }

            val currentTicks = System.currentTimeMillis()
            val elapsedMs = currentTicks - currentSampleStartTime

            if (elapsedMs >= threadAdjustmentIntervalMs / 2) {
                // Enough time has passed, let's call hill climbing algorithm
                val totalNumCompletions = completionCount.value
                val numCompletions = totalNumCompletions - priorCompletionCount
                val oldNumThreadsGoal = getNumThreadsGoal(counts)

                val (newNumThreadsGoal, newThreadAdjustmentIntervalMs) = hillClimber.update(oldNumThreadsGoal, elapsedMs / 1000.0, numCompletions)

                threadAdjustmentIntervalMs = newThreadAdjustmentIntervalMs

                if (oldNumThreadsGoal != newNumThreadsGoal) {
                    // There is an actual adjustment, we have to update data.
                    interlockedSetNumThreadsGoal(newNumThreadsGoal)
                    if (newNumThreadsGoal > oldNumThreadsGoal) {
                        addWorker = true
                    }
                }

                priorCompletionCount = totalNumCompletions
                nextCompletedWorkRequestsTime = currentTicks + threadAdjustmentIntervalMs
                currentSampleStartTime = currentTicks
            }
        } finally {
            threadAdjustmentLock.unlock()
        }

        if (addWorker) {
            maybeAddWorker()
        }
    }

    // This is the main function that manages actual number of working/created threads
    // based on current goal and potential blocked threads.
    private fun maybeAddWorker() {
        var counts = threadCounts.get()

        var numExistingThreads: Int
        var numProcessingWork: Int
        var newNumExistingThreads: Int
        var newNumProcessingWork: Int

        while (true) {
            numProcessingWork = getNumProcessingWork(counts)
            if (numProcessingWork >= getNumThreadsGoal(counts)) {
                // Already enough workers, return.
                return
            }

            newNumProcessingWork = numProcessingWork + 1

            numExistingThreads = getNumExistingThreads(counts)
            newNumExistingThreads = max(numExistingThreads, newNumProcessingWork)

            var newCounts = counts

            newCounts = updateNumProcessingWork(newCounts, newNumProcessingWork)
            newCounts = updateNumExistingThreads(newCounts, newNumExistingThreads)

            val oldCounts = threadCounts.compareAndExchange(counts, newCounts)

            if (oldCounts == counts) {
                // CAS finished successfully, we can finish.
                break
            }

            // We lost a race, try again.
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

    // Reduce the number of working workers by one, but maybe add back a worker (possibly this thread)
    // if a thread request comes in while we are marking this thread as not working.
    private fun removeWorkingWorker() {
        var counts = threadCounts.get()

        // [TODO] Verify if atomic decrement is sufficient.
        while (true) {
            val newCounts = decrementNumProcessingWork(counts)
            val countsBeforeUpdate = threadCounts.compareAndExchange(counts, newCounts)
            if (countsBeforeUpdate == counts) {
                break
            }
            counts = countsBeforeUpdate
        }

        // It's possible that we decided we had thread requests just before a request came in,
        // but reduced the worker count *after* the request came in.  In this case, we might
        // miss the notification of a thread request. So we wake up a thread (maybe this one!)
        // if there is work to do.
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

    // Wake up or create gate thread if needed.
    private fun ensureGateThreadRunning() {
        if (gateThreadRunningState.value != getRunningStateForNumRuns(MAX_RUNS)) {
            val numRunsMask: Int = gateThreadRunningState.getAndSet(getRunningStateForNumRuns(MAX_RUNS))
            if (numRunsMask == getRunningStateForNumRuns(0)) {
                runGateThreadEvent.set()
            } else if ((numRunsMask and GATE_THREAD_RUNNING_MASK) == 0) {
                createGateThread()
            }
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

        // [TODO] Decide corePoolSize or MIN_SUPPORTED_POOL_SIZE
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

            // [TODO] Handle memory usage here if needed (it happens in .NET)

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
        val delay = System.currentTimeMillis() - lastDequeueTime

        // [TODO] Handle CPU usage (it happens in .NET)

        val minimumDelay = GATE_ACTIVITIES_PERIOD_MS

        return delay > minimumDelay
    }

    // Notifies scheduler that a thread starts executing blocking task, which means that
    // additional workers might be required.
    private fun notifyThreadBlocked() {
        var shouldWakeGateThread = false

        threadAdjustmentLock.withLock {
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

        if (shouldWakeGateThread) {
            wakeGateThread()
        }
    }

    private fun notifyThreadUnblocked() {
        var shouldWakeGateThread = false

        threadAdjustmentLock.withLock {
            require(numBlockingTasks > 0)
            numBlockingTasks--

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

    // Increases the number of workers that are allowed to work concurrently.
    private fun releasePermits(permits: Int) {
        if (USE_DOTNET_SEMAPHORE) {
            semaphoreDotnet.release(permits)
        } else {
            semaphore.release(permits)
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
        val localQueueDotnet = WorkStealingQueue()
        init {
            isDaemon = true
            if (!USE_KOTLIN_LOCAL_QUEUES) {
                workStealingQueueList.add(localQueueDotnet)
            }
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

        val workerCtl = atomic(CLAIMED)

        private val stolenTask: ObjectRef<Task?> = ObjectRef()
        private var rngState = Random.nextInt()
        private var minDelayUntilStealableTasksNs = 0L
        private var shouldSpinWait = true

        inline val scheduler get() = this@CoroutineScheduler

        override fun run() = runWorker()

        private fun runWorker() {
            while (!isTerminated) {
                shouldSpinWait = true
                while (!isTerminated) {
                    if (tryAcquirePermit(shouldSpinWait)) {
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
                if (ENABLE_STARVATION_DETECTION) {
                    lastDequeueTime = System.currentTimeMillis()
                }

                if (!processBatchOfWork()) {
                    // ShouldStopProcessingWorkNow() caused the thread to stop processing work, and it would have
                    // already removed this worker in the counts. This typically happens when hill climbing
                    // decreases the worker thread count goal.
                    alreadyRemovedWorkingWorker = true
                    break
                }

                if (numRequestedWorkers.value <= 0) {
                    break
                }

                // In cases with short bursts of work, worker threads are being released and entering processBatchOfWork
                // very quickly, not finding much work, and soon afterward going back, causing extra thrashing on
                // data and some atomic operations, and similarly when the scheduler runs out of work. Since
                // there is a pending request for work, introduce a slight delay before serving the next request.
                yield()
            }

            // Don't spin-wait on the semaphore next time if the thread was actively stopped from processing work,
            // as it's unlikely that the worker thread count goal would be increased again so soon afterward that
            // the semaphore would be released within the spin-wait window.
            shouldSpinWait = !alreadyRemovedWorkingWorker

            if (!alreadyRemovedWorkingWorker) {
                // If we woke up but couldn't find a request, or ran out of work items to process, we need to update
                // the number of working workers to reflect that we are done working for now.
                removeWorkingWorker()
            }
        }

        private fun notifyWorkItemComplete(currentTimeMs: Long): Boolean {
            notifyWorkItemProgress(currentTimeMs)
            return !shouldStopProcessingWorkNow()
        }

        private fun notifyWorkItemProgress(currentTimeMs: Long) {
            completionCount.incrementAndGet()
            if (ENABLE_STARVATION_DETECTION) {
                lastDequeueTime = currentTimeMs
            }
            if (shouldAdjustMaxWorkersActive(currentTimeMs)) {
                adjustMaxWorkersActive()
            }
        }

        // Returns if the current thread should stop processing work for the scheduler.
        // A thread should stop processing work when work remains only when
        // there are more worker threads in the scheduler than we currently want.
        private fun shouldStopProcessingWorkNow(): Boolean {
            var counts = threadCounts.get()
            while (true) {
                // When there are more threads processing work than the thread count goal, it may have been decided
                // to decrease the number of threads. Stop processing if the counts can be updated. We may have more
                // threads existing than the thread count goal and that is ok, the cold ones will eventually time out if
                // the thread count goal is not increased again.
                if (getNumProcessingWork(counts) <= getNumThreadsGoal(counts)) {
                    return false
                }

                val newCounts = decrementNumProcessingWork(counts)
                val oldCounts = threadCounts.compareAndExchange(counts, newCounts)

                if (oldCounts == counts) {
                    return true
                }

                counts = oldCounts

                // [TODO] Remove this, doesn't compile without it
                require(counts == oldCounts)
            }
        }

        // Checks if we should call hill climbing algorithm to potentially change
        // number of workers.
        private fun shouldAdjustMaxWorkersActive(currentTimeMs: Long): Boolean {
            if (!ENABLE_HILL_CLIMBING) {
                return false
            }

            // Not enough time passed
            if (currentTimeMs < nextCompletedWorkRequestsTime) {
                return false
            }

            // Avoid trying to adjust the thread count goal if there are already
            // more threads than the thread count goal. In that situation, hill climbing must have
            // previously decided to decrease the thread count goal, so we need to wait
            // until the system responds to that change before calling hill climbing again.
            val counts = threadCounts.get()
            if (getNumProcessingWork(counts) > getNumThreadsGoal(counts)) {
                return false
            }

            // Skip hill climbing when there is a pending blocking adjustment. Hill climbing may
            // otherwise bypass the blocking adjustment heuristics.
            return pendingBlockingAdjustment == PendingBlockingAdjustment.None
        }

        // Acquires permit, thus being able to process work.
        private fun tryAcquirePermit(spinWait: Boolean): Boolean {
            return try {
                if (USE_DOTNET_SEMAPHORE) {
                    semaphoreDotnet.wait(idleWorkerKeepAliveNs / 1_000_000, spinWait)
                } else {
                    semaphore.tryAcquire(idleWorkerKeepAliveNs, TimeUnit.NANOSECONDS)
                }
            } catch (e: InterruptedException) {
                this.interrupt()
                false
            }
        }

        private fun pollLocalQueue(): Task? {
            return if (USE_KOTLIN_LOCAL_QUEUES) {
                localQueue.poll()
            } else {
                localQueueDotnet.localPop()
            }
        }

        // Transfer all the work to global queue. It's useful when the thread is finishing dispatching early,
        // with more work in its local queue.
        private fun transferLocalWork() {
            while (true) {
                val task = pollLocalQueue() ?: break
                addToGlobalQueue(task)
            }
        }

        private fun findTask(): Task? {
            val globalFirst = nextInt(2 * corePoolSize) == 0

            if (globalFirst) pollGlobalQueues()?.let { return it }
            pollLocalQueue()?.let { return it }
            if (!globalFirst) pollGlobalQueues()?.let { return it }

            val missedSteal = ObjectRef<Boolean>()
            missedSteal.element = false

            val result = trySteal()

            // No work.
            // If we missed a steal, though, there may be more work in the queue.
            // Instead of looping around and trying again, we'll just request another thread. Hopefully the thread
            // that owns the contended queue will pick up its own tasks in the meantime,
            // which will be more efficient than this thread doing it anyway.
            if (result.isFailure) {
                ensureThreadRequested()
            }

            return result.getOrNull()
        }

        // Invoked when the thread's wait timed out. We are potentially shutting down this thread.
        // We are going to decrement the number of existing threads to no longer include this one
        // and then change the max number of threads in the thread pool to reflect that we don't need as many
        // as we had. Finally, we are going to tell hill climbing that we changed the max number of threads.
        private fun shouldExitWorker(): Boolean {
            threadAdjustmentLock.lock()
            try {
                var counts = threadCounts.get()
                while (true) {
                    // Since this thread is currently registered as an existing thread, if more work comes in meanwhile,
                    // this thread would be expected to satisfy the new work. Ensure that numExistingThreads is not
                    // decreased below numProcessingWork, as that would be indicative of such a case.
                    if (getNumExistingThreads(counts) <= getNumProcessingWork(counts)) {
                        // In this case, enough work came in that this thread should not time out and should go back to work.
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
                }
            } finally {
                threadAdjustmentLock.unlock()
            }
        }

        private fun tryTerminateWorker() {
            synchronized(workers) {
                if (isTerminated) return
                if (createdWorkers.value <= corePoolSize)
                if (!workerCtl.compareAndSet(PARKED, TERMINATED)) return
                val oldIndex = indexInArray
                indexInArray = 0

                val lastIndex = createdWorkers.getAndDecrement()
                if (lastIndex != oldIndex) {
                    val lastWorker = workers[lastIndex]!!
                    workers.setSynchronized(oldIndex, lastWorker)
                    lastWorker.indexInArray = oldIndex
                }

                workers.setSynchronized(lastIndex, null)
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
        
        // Returns true if this thread did as much work as was available or its quantum has expired.
        // Returns false if this thread has stopped early.
        private fun processBatchOfWork(): Boolean {
            // Before dequeue of the first work item, acknowledge that the thread request has been satisfied.
            markThreadRequestSatisfied()

            var startTickCount = System.currentTimeMillis()
            var firstLoop = true

            // Loop until our quantum expires or there is no work.
            while (true) {
                val workItem = findTask() ?:
                    if (ENABLE_MIN_DELAY_UNTIL_STEALING && minDelayUntilStealableTasksNs != 0L) {
                        // We didn't take any tasks, but there are some to be stolen in near future.
                        // Instead of returning, we can wait and try again after a small period of time.
                        interrupted()
                        LockSupport.parkNanos(minDelayUntilStealableTasksNs)
                        minDelayUntilStealableTasksNs = 0L
                        continue
                    } else {
                        // There are no tasks to process, return normally.
                        return true
                    }

                if (firstLoop) {
                    // A task was successfully dequeued, and there may be more tasks to process. Request a thread to
                    // parallelize processing of tasks, before processing more. Following this, it is the
                    // responsibility of the new thread and other enqueuing work to request more threads as necessary.
                    // The parallelization may be necessary here for correctness if the task blocks for some
                    // reason that may have a dependency on other queued tasks.
                    ensureThreadRequested()
                    firstLoop = false
                }

                minDelayUntilStealableTasksNs = 0L
                executeWorkItem(workItem)

                // Notify the scheduler that we executed this task. This is also our opportunity to ask
                // whether Hill Climbing wants us to return the thread to the pool or not.
                val currentTickCount = System.currentTimeMillis()
                if (!notifyWorkItemComplete(currentTickCount)) {
                    // This thread is being parked and may remain inactive for a while. Transfer any thread-local work items
                    // to ensure that they would not be heavily delayed. Tell the caller that this thread was requested to stop
                    // processing work items.
                    transferLocalWork()
                    return false
                }

                // Check if the dispatch quantum has expired
                if (currentTickCount - startTickCount < PROCESS_BATCH_QUANTUM_MS) {
                    continue
                }

                // This method will continue to dispatch work items. Refresh the start tick count for the next dispatch quantum.
                startTickCount = currentTickCount
            }
        }

        private fun pollGlobalQueues(): Task? {
            return if (USE_KOTLIN_GLOBAL_QUEUE) {
                globalQueue.removeFirstOrNull()
            } else {
                globalQueueJava.poll()
            }
        }

        private fun trySteal(stealingMode: StealingMode = STEAL_ANY): ChannelResult<Task?> {
            val created: Int = createdWorkers.value

            if (created < 2) {
                return ChannelResult.success(null)
            }

            var currentIndex = nextInt(created)
            var minDelay = Long.MAX_VALUE
            var missedSteal = false
            repeat(created) {
                ++currentIndex
                if (currentIndex > created) currentIndex = 1
                val worker = workers[currentIndex]
                if (worker !== null && worker !== this) {
                    if (USE_KOTLIN_LOCAL_QUEUES) {
                        val stealResult = worker.localQueue.trySteal(stealingMode, stolenTask)
                        if (stealResult == TASK_STOLEN) {
                            val result = stolenTask.element
                            stolenTask.element = null
                            return ChannelResult.success(result)
                        } else {
                            if (worker.localQueue.size > 0) {
                                missedSteal = true
                            }
                            if (stealResult > 0) {
                                minDelay = min(minDelay, stealResult)
                            }
                        }
                    } else {
                        val result = worker.localQueueDotnet.trySteal()
                        if (result.isFailure) {
                            missedSteal = true
                        } else {
                            result.getOrNull()?.let { return ChannelResult.success(it) }
                        }

                    }
                }
            }

            minDelayUntilStealableTasksNs = if (minDelay != Long.MAX_VALUE) minDelay else 0
            return if (missedSteal) {
                ChannelResult.failure()
            } else {
                ChannelResult.success(null)
            }
        }

        private fun executeWorkItem(workItem: Task) {
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

                    //[TODO] Here log CPU utilization if possible

                    if (ENABLE_STARVATION_DETECTION &&
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

// [TODO] Add support.
@JvmName("mayNotBlock")
internal fun mayNotBlock(thread: Thread) = thread is CoroutineScheduler.Worker
//internal fun mayNotBlock(thread: Thread) = thread is CoroutineScheduler.Worker &&
//    thread.state == CoroutineScheduler.WorkerState.CPU_ACQUIRED