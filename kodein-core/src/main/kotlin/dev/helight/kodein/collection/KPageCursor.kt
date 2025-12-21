package dev.helight.kodein.collection

data class KPageCursor(
    val page: Int = 1,
    val pageSize: Int = 20
) {
    val skip: Int
        get() = (page - 1) * pageSize
    val limit: Int
        get() = pageSize
}