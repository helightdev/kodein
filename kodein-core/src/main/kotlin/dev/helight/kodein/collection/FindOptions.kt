package dev.helight.kodein.collection

data class FindOptions(
    var skip: Int = 0,
    var limit: Int = Int.MAX_VALUE,
    var sort: MutableList<Sort> = mutableListOf(),
    var fields: MutableSet<String> = mutableSetOf()
) {
    fun include(vararg paths: String): FindOptions {
        fields.addAll(paths)
        return this
    }

    fun sortByDesc(path: String): FindOptions {
        sort.add(Sort(path, ascending = false))
        return this
    }

    fun sortByAsc(path: String): FindOptions {
        sort.add(Sort(path, ascending = true))
        return this
    }

    data class Sort(
        val path: String,
        val ascending: Boolean = true
    )
}