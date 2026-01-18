package dev.helight.kodein.memory

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.File
import java.nio.file.Files

class SafeByteArrayFileStoreTest {

    @Test
    fun `Test basic store and load`() = runBlocking {
        val tempDir = Files.createTempDirectory("safe-store-test").toFile()
        val file = File(tempDir, "data.bin")
        val data = "Hello Safe Store".toByteArray()

        try {
            SafeByteArrayFileStore.store(data, file)
            assertTrue(file.exists())
            val loaded = SafeByteArrayFileStore.load(file)
            assertArrayEquals(data, loaded)
        } finally {
            tempDir.deleteRecursively()
        }
    }

    @Test
    fun `Test backup and temp files`() = runBlocking {
        val tempDir = Files.createTempDirectory("safe-store-backup-test").toFile()
        val file = File(tempDir, "data.bin")
        val tempFile = File(tempDir, "data.bin.tmp")
        val backupFile = File(tempDir, "data.bin.bak")

        val initialData = "Initial Data".toByteArray()
        val newData = "New Data".toByteArray()

        try {
            // Store initial data
            SafeByteArrayFileStore.store(initialData, file, tempFile, backupFile)
            assertTrue(file.exists())
            assertFalse(backupFile.exists(), "Backup should not exist yet as there was no previous file")

            // Store new data
            SafeByteArrayFileStore.store(newData, file, tempFile, backupFile)
            assertTrue(file.exists())
            assertTrue(backupFile.exists(), "Backup should exist now")
            assertArrayEquals(initialData, backupFile.readBytes(), "Backup should contain initial data")
            assertArrayEquals(newData, file.readBytes(), "File should contain new data")
        } finally {
            tempDir.deleteRecursively()
        }
    }

    @Test
    fun `Test load non-existent file`() = runBlocking {
        val tempDir = Files.createTempDirectory("safe-store-load-test").toFile()
        val file = File(tempDir, "nonexistent.bin")

        try {
            val loaded = SafeByteArrayFileStore.load(file)
            assertEquals(null, loaded)
        } finally {
            tempDir.deleteRecursively()
        }
    }
}
