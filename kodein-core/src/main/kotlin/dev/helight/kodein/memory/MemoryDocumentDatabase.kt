package dev.helight.kodein.memory


import dev.helight.kodein.Kodein
import dev.helight.kodein.collection.DocumentCollection
import dev.helight.kodein.collection.DocumentDatabase
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
        val state = SerializedStorage(
            collections = collections.mapValues { (_, collection) ->
                collection.dumpBytes()
            })
        return kodein.encodeBinary(state)
    }

    suspend fun loadBytes(bytes: ByteArray) = mutex.withLock {
        val state = kodein.decodeBinary<SerializedStorage>(bytes)
        collections.clear()
        for ((name, colBytes) in state.collections) {
            val collection = MemoryDocumentCollection(kodein)
            collection.loadBytes(colBytes)
            collections[name] = collection
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

    fun open() = runBlocking {
        if (path != null) mutex.withLock {
            val file = File(path)
            if (file.exists()) {
                val bytes = file.readBytes()
                loadBytes(bytes)
            }
        }
    }

    override fun close() = runBlocking {
        if (path != null) {
            val file = File(path)
            val bytes = dumpBytes()
            file.writeBytes(bytes)
        }
    }
}


