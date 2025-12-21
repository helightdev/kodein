package dev.helight.kodein.collection

import dev.helight.kodein.Kodein
import java.io.Closeable

interface DocumentDatabase : Closeable {

    val kodein: Kodein

    suspend fun getCollection(name: String): DocumentCollection
    suspend fun createCollection(name: String): DocumentCollection
    suspend fun dropCollection(name: String): Boolean
    suspend fun listCollections(): Set<String>

    fun withNamespace(namespace: String): DocumentDatabase = NamespacedDocumentDatabase(namespace, this, kodein)
}