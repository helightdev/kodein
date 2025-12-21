package dev.helight.kodein.collection

import dev.helight.kodein.KDocument
import java.util.function.IntFunction

class KPage<T>(
    val itemCount: Long,
    val page: Int,
    val pageSize: Int,
    val items: List<T>
) : List<T> by items {
    val pageCount: Int
        get() = if (itemCount == 0L) 1 else ((itemCount - 1) / pageSize + 1).toInt()

    val cursor: KPageCursor
        get() = KPageCursor(page = page, pageSize = pageSize)

    val next: KPageCursor?
        get() = if (page < pageCount) KPageCursor(page + 1, pageSize) else null

    val previous: KPageCursor?
        get() = if (page > 1) KPageCursor(page - 1, pageSize) else null

    fun <R> mapItems(transform: (T) -> R): KPage<R> = KPage(
        itemCount = itemCount,
        page = page,
        pageSize = pageSize,
        items = items.map(transform)
    )

    @Deprecated("Use items property instead", ReplaceWith("items.toTypedArray()"))
    override fun <T : Any?> toArray(generator: IntFunction<Array<out T?>?>): Array<out T?>? {
        return super.toArray(generator)
    }
}