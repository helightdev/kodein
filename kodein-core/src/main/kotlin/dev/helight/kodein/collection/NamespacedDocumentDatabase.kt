package dev.helight.kodein.collection

import dev.helight.kodein.Kodein

class NamespacedDocumentDatabase(
    private val namespace: String,
    private val delegate: DocumentDatabase,
    override val kodein: Kodein
) : DocumentDatabase {
    private fun namespacedName(name: String): String {
        return "${namespace}_$name"
    }

    override suspend fun getCollection(name: String): DocumentCollection {
        return delegate.getCollection(namespacedName(name))
    }

    override suspend fun createCollection(name: String): DocumentCollection {
        return delegate.createCollection(namespacedName(name))
    }

    override suspend fun dropCollection(name: String): Boolean {
        return delegate.dropCollection(namespacedName(name))
    }

    override suspend fun listCollections(): Set<String> {
        val prefix = "${namespace}_"
        return delegate.listCollections()
            .filter { it.startsWith(prefix) }
            .map { it.removePrefix(prefix) }
            .toSet()
    }

    override fun toString(): String {
        return "${delegate}:$namespace"
    }

    override fun close() {
        // No-op
    }
}