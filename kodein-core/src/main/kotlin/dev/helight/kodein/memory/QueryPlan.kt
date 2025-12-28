package dev.helight.kodein.memory

import dev.helight.kodein.collection.Filter

/**
 * Represents a query execution plan
 */
sealed class QueryPlan {
    abstract val estimatedCost: Double
    abstract val filter: Filter?

    /**
     * Full collection scan - no index used
     */
    data class FullScan(
        override val filter: Filter?,
        val documentCount: Int
    ) : QueryPlan() {
        override val estimatedCost: Double = documentCount.toDouble()
    }

    /**
     * Index scan - uses a single index
     */
    data class IndexScan(
        val indexName: String,
        val indexedFilter: Filter.Field,
        val remainingFilter: Filter?,
        val expectedResults: Int
    ) : QueryPlan() {
        override val filter: Filter? = remainingFilter
        override val estimatedCost: Double = expectedResults.toDouble() * 1.1 // slight overhead for index lookup
    }

    /**
     * Text index scan - uses text index
     */
    data class TextIndexScan(
        val indexedFields: Set<String>,
        val textFilter: Filter.Field.Text,
        val remainingFilter: Filter?,
        val expectedResults: Int
    ) : QueryPlan() {
        override val filter: Filter? = remainingFilter
        override val estimatedCost: Double = expectedResults.toDouble() * 1.2 // text search overhead
    }

    /**
     * Composite plan - uses multiple strategies
     */
    data class CompositePlan(
        val indexScans: List<IndexScan>,
        val textScans: List<TextIndexScan>,
        val remainingFilter: Filter?,
        val expectedResults: Int
    ) : QueryPlan() {
        override val filter: Filter? = remainingFilter
        override val estimatedCost: Double = expectedResults.toDouble() * 1.05
    }
}

/**
 * Explanation of the query plan
 */
data class QueryExplanation(
    val planType: String,
    val indexesUsed: List<String>,
    val estimatedCost: Double,
    val optimized: Boolean,
    val details: Map<String, Any>
)
