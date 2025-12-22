package dev.helight.kodein

import dev.helight.kodein.collection.DocumentDatabase

interface DocumentDatabaseScope {
    fun databaseScope(block: suspend DocumentDatabase.() -> Unit)

    val isStrict
        get() = true
}