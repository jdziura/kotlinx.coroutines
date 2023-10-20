/*
 * Copyright 2016-2023 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.scheduling

import kotlinx.coroutines.Runnable
import java.io.*
import java.util.concurrent.*

internal interface Scheduler : Executor, Closeable {
    fun dispatch(block: Runnable, taskContext: TaskContext = NonBlockingContext, tailDispatch: Boolean = false)
    fun createTask(block: Runnable, taskContext: TaskContext): Runnable
    fun shutdown(timeout: Long)
}