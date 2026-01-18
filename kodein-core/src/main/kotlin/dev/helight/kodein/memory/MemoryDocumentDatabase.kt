package dev.helight.kodein.memory


import dev.helight.kodein.Kodein
import dev.helight.kodein.collection.DocumentCollection
import dev.helight.kodein.collection.DocumentDatabase
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.Serializable
import java.io.File
import kotlin.collections.set

class MemoryDocumentDatabase(
    override val kodein: Kodein,
    val path: String? = null
) : DocumentDatabase {
    private val collections = mutableMapOf<String, MemoryDocumentCollection>()
    private val mutex = Mutex()

    @Serializable
    private class SerializedStorage(
        val collections: Map<String, ByteArray>
    )

    suspend fun dumpBytes(): ByteArray = mutex.withLock {
        return dumpBytesInternal()
    }

    private suspend fun dumpBytesInternal(): ByteArray = coroutineScope {
        val state = collections.map {
            async(Dispatchers.Default) { it.key to it.value.dumpBytes() }
        }.awaitAll().toMap()

        kodein.encodeBinary(SerializedStorage(state))
    }

    suspend fun loadBytes(bytes: ByteArray) = mutex.withLock {
        loadBytesInternal(bytes)
    }

    private suspend fun loadBytesInternal(bytes: ByteArray) {
        val state = kodein.decodeBinary<SerializedStorage>(bytes)
        collections.clear()
        for ((name, colBytes) in state.collections) {
            val collection = MemoryDocumentCollection(kodein)
            collections[name] = collection
            collection.loadBytes(colBytes)
        }
    }


    override suspend fun getCollection(name: String): DocumentCollection = mutex.withLock {
        return collections.getOrPut(name) { MemoryDocumentCollection(kodein) }
    }

    override suspend fun createCollection(name: String): DocumentCollection = mutex.withLock {
        val collection = MemoryDocumentCollection(kodein)
        collections[name] = collection
        return collection
    }

    override suspend fun dropCollection(name: String): Boolean = mutex.withLock {
        return collections.remove(name) != null
    }

    override suspend fun listCollections(): Set<String> {
        return collections.keys
    }

    fun open() = runBlocking { openSuspended() }
    override fun close() = runBlocking { closeSuspended() }

    suspend fun openSuspended() {
        if (path != null) mutex.withLock {
            val file = File(path)
            val bytes = SafeByteArrayFileStore.load(file)
            bytes?.let { loadBytesInternal(it) }
        }
    }

    suspend fun closeSuspended() {
        if (path != null) mutex.withLock {
            val file = File(path)
            val bytes = dumpBytesInternal()
            SafeByteArrayFileStore.store(bytes, file)
        }
    }
}


