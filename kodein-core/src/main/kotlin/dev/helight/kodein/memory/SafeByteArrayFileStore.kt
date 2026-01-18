package dev.helight.kodein.memory

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withContext
import java.io.File

object SafeByteArrayFileStore {

    suspend fun store(byteArray: ByteArray, file: File) {
        val tempFile = File(file.absolutePath + ".tmp")
        val backupFile = File(file.absolutePath + ".bak")
        store(byteArray, file, tempFile, backupFile)
    }

    suspend fun store(byteArray: ByteArray, file: File, tempFile: File, backupFile: File) = coroutineScope {
        val backupJob = async(Dispatchers.IO) {
            if (file.exists()) {
                file.copyTo(backupFile, overwrite = true)
            }
        }
        val writeJob = async(Dispatchers.IO) {
            tempFile.writeBytes(byteArray)
        }
        awaitAll(backupJob, writeJob)

        withContext(Dispatchers.IO) {
            tempFile.renameTo(file)
        }
    }

    suspend fun load(file: File): ByteArray? = withContext(Dispatchers.IO) {
        if (file.exists()) {
            file.readBytes()
        } else {
            null
        }
    }
}