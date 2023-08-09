/*
 * Copyright 2016-2023 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.scheduling

internal class AutoResetEvent(private var open: Boolean) {
    private val monitor = Object()
    fun waitOne() {
        synchronized(monitor) {
            while (!open) {
                monitor.wait()
            }
            open = false
        }
    }

    fun waitOne(timeout: Long): Boolean {
        synchronized(monitor) {
            val timestamp = System.currentTimeMillis()
            while (!open) {
                monitor.wait(timeout)
                if (System.currentTimeMillis() - timestamp >= timeout) {
                    open = false
                    return false
                }
            }
            open = false
            return true
        }
    }

    fun set() {
        synchronized(monitor) {
            open = true
            monitor.notify()
        }
    }
}