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
import java.util.concurrent.locks.LockSupport
import kotlin.jvm.internal.Ref.ObjectRef
import kotlin.math.*
import kotlin.random.Random

/**
 * If enabled, the scheduler dynamically adjusts the number of active worker threads based on
 * current throughput, trying to maximize it with a minimum number of workers.
 */
internal const val ENABLE_HILL_CLIMBING = true

/**
 * If enabled, Gate Thread occasionally injects a thread if no work has been done for this time period.
 */
internal const val ENABLE_STARVATION_DETECTION = false

/**
 * If enabled, tasks added to local queues will have to wait for a short period of time until they
 * become stealable. This feature works only with Kotlin queues.
 */
internal const val ENABLE_MIN_DELAY_UNTIL_STEALING = false

/**
 * If set to true, allows up to CPU-count active thread requests (entering ensureThreadRequested()).
 * Otherwise, only 1 request is allowed at a time.
 */
internal const val ENABLE_CONCURRENT_THREAD_REQUESTS = false

/**
 * If set to true, the scheduler will be allowed to reduce the number of threads to 1
 * (or 1 + the number of blocking tasks), potentially lowering the thread count significantly.
 * If set to false, it will keep a lower bound of 'corePoolSize' threads. Enabling this option is useful to allow
 * the Hill Climber to reduce the number of threads very low if it is determined to be optimal.
 */
internal const val IGNORE_MIN_THREADS = false

/**
 * If set to false, the code will use the ported .NET implementation.
 */
internal const val USE_KOTLIN_LOCAL_QUEUES = false

/**
 * If set to false, the code will use the JAVA ConcurrentLinkedQueue.
 */
internal const val USE_KOTLIN_GLOBAL_QUEUE = false

/**
 * If set to true, uses a simple spinlock inside local queues; otherwise, uses a reentrant lock.
 */
internal const val USE_DOTNET_QUEUE_SPINLOCK = false

/**
 * If set to true, uses a custom .NET semaphore for managing the number of active threads.
 * It uses a JAVA semaphore under the hood but wraps it with larger logic of spin waiting.
 * If set to false, uses pure JAVA semaphore.
 */
internal const val USE_DOTNET_SEMAPHORE = false

internal const val LOG_MAJOR_HC_ADJUSTMENTS = false

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
        /**
         * Constants used to manage Gate Thread.
         */
        private const val MAX_RUNS = 2
        private const val GATE_THREAD_RUNNING_MASK = 0x4
        private const val GATE_ACTIVITIES_PERIOD_MS = DelayHelper.GATE_ACTIVITIES_PERIOD_MS
        private const val DELAY_STEP_MS = 25L
        private const val MAX_DELAY_MS = 250L

        /**
         * Values used during blocking adjustment.
         */
        private val threadsToAddWithoutDelay = AVAILABLE_PROCESSORS
        private val threadsPerDelayStep = AVAILABLE_PROCESSORS

        /**
         * Constants used for retrieving and updating values from threadCount state.
         */
        private const val SHIFT_LENGTH = 16
        private const val SHIFT_PROCESSING_WORK =   0
        private const val SHIFT_EXISTING_THREADS =  SHIFT_LENGTH
        private const val SHIFT_THREADS_GOAL =      SHIFT_LENGTH * 2
        private const val MASK_PROCESSING_WORK =    (1L shl SHIFT_LENGTH) - 1
        private const val MASK_EXISTING_THREADS =   MASK_PROCESSING_WORK shl SHIFT_EXISTING_THREADS
        private const val MASK_THREADS_GOAL =       MASK_PROCESSING_WORK shl SHIFT_THREADS_GOAL

        /**
         * Determines the time a thread will spin on .NET-based semaphore until it registers as a waiter.
         * Doesn't do anything if using pure JAVA Semaphore.
         */
        private const val SEMAPHORE_SPIN_COUNT = 16

        internal const val MIN_SUPPORTED_POOL_SIZE = 1
        internal const val MAX_SUPPORTED_POOL_SIZE = (1 shl SHIFT_LENGTH) - 2
    }

    /**
     * The minimum number of threads guaranteed to be available for CPU tasks.
     */
    private val lowerThreadsBound = if (IGNORE_MIN_THREADS) 1 else corePoolSize

    /**
     * State variable that holds the current thread counts:
     *   - Number of active workers (processing work).
     *   - Number of created workers (including active and those waiting for permits).
     *   - Goal number of threads (adjusted by hill climbing algorithm).
     *
     * By default, the goal is set to match the number of CPU cores.
     */
    private val startingThreadCountsValue = 0L.setNumThreadsGoal(corePoolSize)
    private val threadCounts = atomic(startingThreadCountsValue)

    /**
     * A counter for the number of outstanding thread requests. Helps limit the maximum
     * number of concurrent requests to reduce contention.
     */
    private val numOutstandingThreadRequests = atomic(0)

    /**
     * The count of workers requested to execute tasks. A worker must take a request
     * before processing a batch of work.
     */
    private val numRequestedWorkers = atomic(0)

    /**
     * Current state indicator for the Gate Thread. Used to ensuring that Gate Thread is running when needed.
     */
    private val gateThreadRunningState = atomic(0)

    /**
     * Total number of tasks completed by worker threads.
     */
    private val completionCount = atomic(0)

    /**
     * Number of workers created. While it may overlap with threadCounts, it is required by the workers array.
     */
    private val createdWorkers = atomic(0)

    /**
     * Helper structures used by the Gate Thread for periodic checks and signaling.
     */
    private val runGateThreadEvent = AutoResetEvent(true)
    private val delayEvent = AutoResetEvent(false)
    private val delayHelper = DelayHelper()

    private val threadAdjustmentLock = ReentrantLock()

    /**
     * Semaphore is used to restrict the number of threads allowed to process work to a specific number of permits.
     */
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
    val workers = ResizableAtomicArray<Worker>((corePoolSize + 1) * 2)

    @JvmField
    val globalQueue = GlobalQueue()

    @JvmField
    val globalQueueJava = ConcurrentLinkedQueue<Task>()


    private val _isTerminated = atomic(false)
    val isTerminated: Boolean inline get() = _isTerminated.value

    /**
     * Returns a lower bound on threads goal (for example, to use by hill climbing algorithm).
     * This property should be accessed only when the 'threadAdjustmentLock' is held.
     */
    val minThreadsGoal: Int
        inline get() {
            return min(numThreadsGoal, targetThreadsForBlockingAdjustment)
        }

    /**
     * Returns the required number of threads to run, taking into account the number of blocking tasks
     * currently being executed. This property should be accessed only when the 'threadAdjustmentLock' is held.
     */
    private val targetThreadsForBlockingAdjustment: Int
        inline get() {
            return if (numBlockingTasks <= 0) {
                lowerThreadsBound
            } else {
                min(lowerThreadsBound + numBlockingTasks, maxPoolSize)
            }
        }

    private inline fun Long.getValue(mask: Long, shift: Int) =
        ((this and mask) shr shift).toInt()

    private inline fun Long.setValue(value: Int, mask: Long, shift: Int) =
        (this and mask.inv()) or (value.toLong() shl shift)

    private val Long.numProcessingWork
        inline get() = getValue(MASK_PROCESSING_WORK, SHIFT_PROCESSING_WORK)
    private val Long.numExistingThreads
        inline get() = getValue(MASK_EXISTING_THREADS, SHIFT_EXISTING_THREADS)
    private val Long.numThreadsGoal
        inline get() = getValue(MASK_THREADS_GOAL, SHIFT_THREADS_GOAL)

    private val numProcessingWork
        inline get() = threadCounts.value.numProcessingWork
    private val numExistingThreads
        inline get() = threadCounts.value.numExistingThreads
    private val numThreadsGoal
        inline get() = threadCounts.value.numThreadsGoal

    private inline fun Long.setNumProcessingWork(value: Int) =
        setValue(value, MASK_PROCESSING_WORK, SHIFT_PROCESSING_WORK)
    private inline fun Long.setNumExistingThreads(value: Int) =
        setValue(value, MASK_EXISTING_THREADS, SHIFT_EXISTING_THREADS)
    private inline fun Long.setNumThreadsGoal(value: Int) =
        setValue(value, MASK_THREADS_GOAL, SHIFT_THREADS_GOAL)

    private inline fun Long.decrementNumProcessingWork() =
        this - (1L shl SHIFT_PROCESSING_WORK)
    private inline fun Long.decrementNumExistingThreads() =
        this - (1L shl SHIFT_EXISTING_THREADS)

    private inline fun decrementNumProcessingWork() =
        threadCounts.addAndGet(-(1L shl SHIFT_PROCESSING_WORK))

    private fun interlockedSetNumThreadsGoal(value: Int): Long {
        threadCounts.loop { counts ->
            val newCounts = counts.setNumThreadsGoal(value)
            if (threadCounts.compareAndSet(counts, newCounts)) {
                return newCounts
            }
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

    /**
     * Requests for a thread to process work. Restricts number of concurrent request, to avoid over-parallelization.
     */
    private fun ensureThreadRequested() {
        if (ENABLE_CONCURRENT_THREAD_REQUESTS) {
            // Restricts the number of requests to number of available processors at a time.
            numOutstandingThreadRequests.loop { cnt ->
                if (cnt < AVAILABLE_PROCESSORS) return
                if (numOutstandingThreadRequests.compareAndSet(cnt, cnt + 1)) {
                    requestWorker()
                    return
                }
            }
        } else {
            // Restricts the number of requests to 1 at a time.
            if (numOutstandingThreadRequests.compareAndSet(0, 1)) {
                requestWorker()
            }
        }
    }

    /**
     * Notifies that a thread request has been satisfied, and allows other requests to be processed.
     */
    private fun markThreadRequestSatisfied() {
        if (ENABLE_CONCURRENT_THREAD_REQUESTS) {
            numOutstandingThreadRequests.loop { cnt ->
                if (cnt <= 0) return
                if (numOutstandingThreadRequests.compareAndSet(cnt, cnt - 1)) {
                    return
                }
            }
        } else {
            numOutstandingThreadRequests.value = 0
        }

    }

    private fun currentWorker(): Worker? = (Thread.currentThread() as? Worker)?.takeIf { it.scheduler == this }

    private fun requestWorker() {
        numRequestedWorkers.incrementAndGet()
        maybeAddWorker()
        ensureGateThreadRunning()
    }

    /**
     * Calls hill climbing algorithm to potentially adjust number of workers.
     */
    private fun adjustMaxWorkersActive() {
        if (!threadAdjustmentLock.tryLock()) {
            // The lock is held by someone else, they will take care of this for us.
            return
        }

        var addWorker = false

        try {
            val counts = threadCounts.value
            if (counts.numProcessingWork > counts.numThreadsGoal ||
                pendingBlockingAdjustment != PendingBlockingAdjustment.None) {
                return
            }

            val currentTicks = System.currentTimeMillis()
            val elapsedMs = currentTicks - currentSampleStartTime

            if (elapsedMs >= threadAdjustmentIntervalMs / 2) {
                // Enough time has passed, let's call hill climbing algorithm
                val totalNumCompletions = completionCount.value
                val numCompletions = totalNumCompletions - priorCompletionCount
                val oldNumThreadsGoal = counts.numThreadsGoal

                val (newNumThreadsGoal, newThreadAdjustmentIntervalMs) = hillClimber.update(oldNumThreadsGoal, elapsedMs / 1000.0, numCompletions)

                threadAdjustmentIntervalMs = newThreadAdjustmentIntervalMs

                if (oldNumThreadsGoal != newNumThreadsGoal) {
                    interlockedSetNumThreadsGoal(newNumThreadsGoal)

                    // If we're increasing the goal, inject a thread. If that thread finds work, it will inject
                    // another thread, etc., until nobody finds work, or we reach the new goal.
                    // If we're reducing the goal, whichever threads notice this first will sleep and timeout themselves.
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

    /**
     * `maybeAddWorker` is the main function used to manage the creation of worker threads based on certain conditions.
     * If certain conditions are satisfied, releases a worker permit and/or creates new worker threads.
     */
    private fun maybeAddWorker() {
        var numExistingThreads: Int
        var numProcessingWork: Int
        var newNumExistingThreads: Int
        var newNumProcessingWork: Int

        while (true) {
            val counts = threadCounts.value
            numProcessingWork = counts.numProcessingWork

            // If there are already enough active workers, return.
            if (numProcessingWork >= counts.numThreadsGoal) return

            // Increment the number of active workers and maybe the number of existing workers.
            newNumProcessingWork = numProcessingWork + 1
            numExistingThreads = counts.numExistingThreads
            newNumExistingThreads = max(numExistingThreads, newNumProcessingWork)

            val newCounts = counts
                .setNumProcessingWork(newNumProcessingWork)
                .setNumExistingThreads(newNumExistingThreads)

            var newCounts2 = counts;
            newCounts2 = newCounts2.setNumProcessingWork(newNumProcessingWork)
            newCounts2 = newCounts2.setNumExistingThreads(newNumExistingThreads)

            require(newCounts == newCounts2)

            if (threadCounts.compareAndSet(counts, newCounts)) {
                break
            }
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

    private fun takeActiveRequest(): Boolean {
        numRequestedWorkers.loop { cnt ->
            if (cnt <= 0) return false
            if (numRequestedWorkers.compareAndSet(cnt, cnt - 1)) {
                return true
            }
        }
    }

    /**
     * Wake up or create gate thread if needed.
     */
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

        var counts = threadCounts.value
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
            interlockedSetNumThreadsGoal(numThreadsGoal)
            hillClimber.forceChange(numThreadsGoal, HillClimbing.StateOrTransition.CooperativeBlocking)

            return (0L to false)
        }

        val configuredMaxThreadsWithoutDelay = min(lowerThreadsBound + threadsToAddWithoutDelay, maxPoolSize)

        do {
            val maxThreadsGoalWithoutDelay = max(configuredMaxThreadsWithoutDelay, min(counts.numExistingThreads, maxPoolSize))
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

    private fun wakeGateThread() {
        delayEvent.set()
        ensureGateThreadRunning()
    }

    /**
     * This function is used to notify a scheduler that a thread has started executing a blocking task,
     * which indicates that additional worker thread might be needed.
     */
    private fun notifyThreadBlocked() {
        var shouldWakeGateThread = false

        threadAdjustmentLock.withLock {
            numBlockingTasks++

            // If there is already pending blocking adjustment, or our threads goal is high enough, don't change.
            if (pendingBlockingAdjustment != PendingBlockingAdjustment.WithDelayIfNecessary &&
                numThreadsGoal < targetThreadsForBlockingAdjustment) {

                // Wake Gate Thread only if there are no other requests.
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

    /**
     * This function is used to notify a scheduler that a thread has finished executing a blocking task,
     * which indicates that there might be adjustment needed.
     */
    private fun notifyThreadUnblocked() {
        var shouldWakeGateThread = false

        threadAdjustmentLock.withLock {
            numBlockingTasks--

            // If there is already pending blocking adjustment, or our threads goal is low enough, don't change.
            if (pendingBlockingAdjustment != PendingBlockingAdjustment.Immediately &&
                numThreadsAddedDueToBlocking > 0 &&
                numThreadsGoal > targetThreadsForBlockingAdjustment) {

                shouldWakeGateThread = true
                pendingBlockingAdjustment = PendingBlockingAdjustment.Immediately
            }
        }

        if (shouldWakeGateThread) {
            wakeGateThread()
        }
    }

    /**
     * Increases the number of threads that are allowed to work concurrently.
     */
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

        val localQueueDotnet = WorkStealingQueue()

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

        /**
         * Determines whether the current thread should stop processing tasks for the scheduler.
         * A thread should stop processing tasks when there are more worker threads in the scheduler than desired.
         */
        private fun shouldStopProcessingWorkNow(): Boolean {
            threadCounts.loop { counts ->
                // When there are more threads processing work than the thread count goal, it may have been decided
                // to decrease the number of threads. Stop processing if the counts can be updated. We may have more
                // threads existing than the thread count goal and that is ok, the cold ones will eventually time out if
                // the thread count goal is not increased again.
                if (counts.numProcessingWork <= counts.numThreadsGoal) {
                    return false
                }

                val newCounts = counts.decrementNumProcessingWork()
                if (threadCounts.compareAndSet(counts, newCounts)) {
                    return true
                }
            }
        }

        /**
         * Determines whether we should invoke the hill climbing algorithm and potentially adjust
         * the number of worker threads.
         */
        private fun shouldAdjustMaxWorkersActive(currentTimeMs: Long): Boolean {
            if (!ENABLE_HILL_CLIMBING) {
                return false
            }

            // Not enough time passed.
            if (currentTimeMs < nextCompletedWorkRequestsTime) {
                return false
            }

            // Avoid trying to adjust the thread count goal if there are already
            // more threads than the thread count goal. In that situation, hill climbing must have
            // previously decided to decrease the thread count goal, so we need to wait
            // until the system responds to that change before calling hill climbing again.
            val counts = threadCounts.value
            if (counts.numProcessingWork > counts.numThreadsGoal) {
                return false
            }

            // Skip hill climbing when there is a pending blocking adjustment. hill climbing may
            // otherwise bypass the blocking adjustment heuristics.
            return pendingBlockingAdjustment == PendingBlockingAdjustment.None
        }

        /**
         * Acquires a permit, granting permission to process work.
         */
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

        /**
         * Transfer all the work to global queue. It's useful when the thread is finishing dispatching early,
         * with more work in its local queue.
         */
        private fun transferLocalWork() {
            while (true) {
                val task = pollLocalQueue() ?: break
                addToGlobalQueue(task)
                ensureThreadRequested()
            }
        }

        private fun findTask(): Task? {
            pollLocalQueue()?.let { return it }
            pollGlobalQueues()?.let { return it }

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

        /**
         * Invoked when the thread's wait timed out. We are potentially shutting down this thread.
         * We are going to decrement the number of existing threads to no longer include this one
         * and then change the max number of threads in the thread pool to reflect that we don't need as many
         * as we had. Finally, we are going to tell hill climbing that we changed the max number of threads.
         */
        private fun shouldExitWorker(): Boolean {
            threadAdjustmentLock.lock()
            try {
                threadCounts.loop { counts ->
                    // Since this thread is currently registered as an existing thread, if more work comes in meanwhile,
                    // this thread would be expected to satisfy the new work. Ensure that numExistingThreads is not
                    // decreased below numProcessingWork, as that would be indicative of such a case.
                    if (counts.numExistingThreads <= counts.numProcessingWork) {
                        // In this case, enough work came in that this thread should not time out and should go back to work.
                        return false
                    }

                    var newCounts = counts.decrementNumExistingThreads()
                    val newNumExistingThreads = newCounts.numExistingThreads
                    val newNumThreadsGoal = max(minThreadsGoal, min(newNumExistingThreads, counts.numThreadsGoal))

                    newCounts = newCounts.setNumThreadsGoal(newNumThreadsGoal)
                    if (threadCounts.compareAndSet(counts, newCounts)) {
                        hillClimber.forceChange(newNumThreadsGoal, HillClimbing.StateOrTransition.ThreadTimedOut)
                        return true
                    }
                }
            } finally {
                threadAdjustmentLock.unlock()
            }
        }

        /**
         * Reduce the number of working workers by one, but maybe add back a worker (possibly this thread)
         * if a thread request comes in while we are marking this thread as not working.
         */
        private fun removeWorkingWorker() {
            decrementNumProcessingWork()

            // It's possible that we decided we had thread requests just before a request came in,
            // but reduced the worker count *after* the request came in.  In this case, we might
            // miss the notification of a thread request. So we wake up a thread (maybe this one!)
            // if there is work to do.
            if (numRequestedWorkers.value > 0) {
                maybeAddWorker()
            }
        }

        private fun tryTerminateWorker() {
            synchronized(workers) {
                if (isTerminated) return

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

        /**
         * Returns true if this thread did as much work as was available.
         * Returns false if this thread has stopped early.
         */
        private fun processBatchOfWork(): Boolean {
            // Before dequeue of the first work item, acknowledge that the thread request has been satisfied.
            markThreadRequestSatisfied()

            var firstLoop = true

            // Loop until there is no work or should exit early.
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
                // whether hill climbing wants us to return the thread to the pool or not.
                val currentTickCount = System.currentTimeMillis()
                if (!notifyWorkItemComplete(currentTickCount)) {
                    // This thread is being parked and may remain inactive for a while. Transfer any thread-local work items
                    // to ensure that they would not be heavily delayed. Tell the caller that this thread was requested to stop
                    // processing work items.
                    transferLocalWork()
                    return false
                }
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

            for (i in 1..created) {
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

                    // Thread count adjustment for cooperative blocking
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

                    // [TODO] Here log CPU utilization if possible.

                    if (ENABLE_STARVATION_DETECTION &&
                        pendingBlockingAdjustment == PendingBlockingAdjustment.None &&
                        numRequestedWorkers.value > 0 &&
                        sufficientDelaySinceLastDequeue()) {

                        var addWorker = false
                        threadAdjustmentLock.withLock {
                            while (true) {
                                val counts = threadCounts.value
                                if (counts.numProcessingWork >= maxPoolSize || counts.numProcessingWork < counts.numThreadsGoal) {
                                    break
                                }

                                val newNumThreadsGoal = counts.numProcessingWork + 1
                                val newCounts = counts.setNumThreadsGoal(newNumThreadsGoal)

                                if (threadCounts.compareAndSet(counts, newCounts)) {
                                    hillClimber.forceChange(newNumThreadsGoal, HillClimbing.StateOrTransition.Starvation)
                                    addWorker = true
                                    break
                                }
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

        // Called by logic to spawn new worker threads if no progress has been made for long enough time.
        // [TODO] Handle CPU usage. In .NET if CPU usage is high, the delay depends on number of threads.
        private fun sufficientDelaySinceLastDequeue(): Boolean {
            val delay = System.currentTimeMillis() - lastDequeueTime
            return delay > GATE_ACTIVITIES_PERIOD_MS
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