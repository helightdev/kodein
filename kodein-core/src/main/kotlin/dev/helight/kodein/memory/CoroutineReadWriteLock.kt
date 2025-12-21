package dev.helight.kodein.memory

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

internal class CoroutineReadWriteLock {

    @PublishedApi
    internal val stateMutex = Mutex()

    @PublishedApi
    internal val writeMutex = Mutex()

    private var readers = 0

    suspend inline fun <T> read(action: suspend () -> T): T {
        enterRead()
        try {
            return action()
        } finally {
            exitRead()
        }
    }

    suspend inline fun <T> write(action: suspend () -> T): T {
        writeMutex.withLock {
            return action()
        }
    }

    @PublishedApi
    internal suspend fun enterRead() {
        stateMutex.withLock {
            readers++
            if (readers == 1) writeMutex.lock()
        }
    }

    @PublishedApi
    internal suspend fun exitRead() {
        stateMutex.withLock {
            readers--
            if (readers == 0) writeMutex.unlock()
        }
    }
}