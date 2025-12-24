package dev.helight.kodein.collection

data class KPageCursor(
    val page: Int = 0,
    val pageSize: Int = 20
) {
    val skip: Int
        get() = page * pageSize
    val limit: Int
        get() = pageSize
}