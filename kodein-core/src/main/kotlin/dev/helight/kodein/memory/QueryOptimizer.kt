package dev.helight.kodein.memory

import dev.helight.kodein.collection.Filter
import dev.helight.kodein.spec.FieldIndexType
import dev.helight.kodein.spec.IndexDefinition

/**
 * Query optimizer that selects the best execution plan based on available indexes
 */
class QueryOptimizer(
    private val indices: Map<String, Pair<IndexDefinition, Any>>, // path -> (definition, index structure)
    private val textIndices: Set<String>,
    private val documentCount: Int
) {
    companion object {
        // Selectivity estimates for cost calculation
        private const val INDEXED_FIELD_SELECTIVITY = 0.1 // 10% of documents
        private const val RANGE_QUERY_SELECTIVITY = 0.3 // 30% of documents
        private const val TEXT_SEARCH_SELECTIVITY = 0.2 // 20% of documents
    }

    /**
     * Optimize a filter and create an execution plan
     */
    fun optimize(filter: Filter?): QueryPlan {
        if (filter == null) {
            return QueryPlan.FullScan(null, documentCount)
        }

        return when (filter) {
            is Filter.And -> optimizeConjunction(filter)
            is Filter.Field -> optimizeSingleField(filter)
            else -> QueryPlan.FullScan(filter, documentCount)
        }
    }

    /**
     * Optimize conjunctive queries (AND filters)
     */
    private fun optimizeConjunction(andFilter: Filter.And): QueryPlan {
        val filters = andFilter.filters
        
        // Separate indexable and non-indexable filters
        val indexableFilters = mutableListOf<Filter.Field>()
        val textFilters = mutableListOf<Filter.Field.Text>()
        val nonIndexableFilters = mutableListOf<Filter>()

        for (f in filters) {
            when (f) {
                is Filter.Field.Text -> {
                    if (textIndices.contains(f.path)) {
                        textFilters.add(f)
                    } else {
                        nonIndexableFilters.add(f)
                    }
                }
                is Filter.Field.Eq, is Filter.Field.Comp, is Filter.Field.In -> {
                    if (indices.containsKey((f as Filter.Field).path)) {
                        indexableFilters.add(f)
                    } else {
                        nonIndexableFilters.add(f)
                    }
                }
                else -> nonIndexableFilters.add(f)
            }
        }

        // Choose the best index-based filter
        val bestIndexFilter = selectBestIndexFilter(indexableFilters)
        val bestTextFilter = textFilters.firstOrNull()

        // Build the plan
        return when {
            bestIndexFilter != null -> {
                val indexName = indices[bestIndexFilter.path]!!.first.indexName
                val expectedResults = estimateResults(bestIndexFilter)
                val remaining = buildRemainingFilter(filters, bestIndexFilter)
                QueryPlan.IndexScan(indexName, bestIndexFilter, remaining, expectedResults)
            }
            bestTextFilter != null -> {
                val expectedResults = estimateTextResults(bestTextFilter)
                val remaining = buildRemainingFilter(filters, bestTextFilter)
                QueryPlan.TextIndexScan(textIndices, bestTextFilter, remaining, expectedResults)
            }
            else -> QueryPlan.FullScan(andFilter, documentCount)
        }
    }

    /**
     * Optimize a single field filter
     */
    private fun optimizeSingleField(fieldFilter: Filter.Field): QueryPlan {
        return when (fieldFilter) {
            is Filter.Field.Text -> {
                if (textIndices.contains(fieldFilter.path)) {
                    val expectedResults = estimateTextResults(fieldFilter)
                    QueryPlan.TextIndexScan(textIndices, fieldFilter, null, expectedResults)
                } else {
                    QueryPlan.FullScan(fieldFilter, documentCount)
                }
            }
            is Filter.Field.Eq, is Filter.Field.Comp, is Filter.Field.In -> {
                if (indices.containsKey(fieldFilter.path)) {
                    val indexName = indices[fieldFilter.path]!!.first.indexName
                    val expectedResults = estimateResults(fieldFilter)
                    QueryPlan.IndexScan(indexName, fieldFilter, null, expectedResults)
                } else {
                    QueryPlan.FullScan(fieldFilter, documentCount)
                }
            }
            else -> QueryPlan.FullScan(fieldFilter, documentCount)
        }
    }

    /**
     * Select the best index filter based on cost estimation
     */
    private fun selectBestIndexFilter(filters: List<Filter.Field>): Filter.Field? {
        if (filters.isEmpty()) return null

        return filters.minByOrNull { filter ->
            estimateResults(filter)
        }
    }

    /**
     * Estimate the number of results for a field filter
     */
    private fun estimateResults(filter: Filter.Field): Int {
        return when (filter) {
            is Filter.Field.Eq -> {
                val indexDef = indices[filter.path]?.first
                if (indexDef?.indexType == FieldIndexType.UNIQUE) {
                    1 // Unique index, expect single result
                } else {
                    (documentCount * INDEXED_FIELD_SELECTIVITY).toInt().coerceAtLeast(1)
                }
            }
            is Filter.Field.In -> {
                val arraySize = filter.value.size
                val perValue = (documentCount * INDEXED_FIELD_SELECTIVITY).toInt()
                (arraySize * perValue).coerceAtMost(documentCount)
            }
            is Filter.Field.Comp -> {
                (documentCount * RANGE_QUERY_SELECTIVITY).toInt().coerceAtLeast(1)
            }
            else -> documentCount
        }
    }

    /**
     * Estimate results for text filter
     */
    private fun estimateTextResults(filter: Filter.Field.Text): Int {
        return (documentCount * TEXT_SEARCH_SELECTIVITY).toInt().coerceAtLeast(1)
    }

    /**
     * Build remaining filter after removing the indexed filter
     */
    private fun buildRemainingFilter(allFilters: List<Filter>, indexedFilter: Filter): Filter? {
        val remaining = allFilters.filter { it != indexedFilter }
        return when {
            remaining.isEmpty() -> null
            remaining.size == 1 -> remaining[0]
            else -> Filter.And(remaining)
        }
    }

    /**
     * Create explanation for a query plan
     */
    fun explain(plan: QueryPlan): QueryExplanation {
        return when (plan) {
            is QueryPlan.FullScan -> QueryExplanation(
                planType = "FULL_SCAN",
                indexesUsed = emptyList(),
                estimatedCost = plan.estimatedCost,
                optimized = false,
                details = mapOf(
                    "documentCount" to plan.documentCount,
                    "reason" to "No suitable index found"
                )
            )
            is QueryPlan.IndexScan -> QueryExplanation(
                planType = "INDEX_SCAN",
                indexesUsed = listOf(plan.indexName),
                estimatedCost = plan.estimatedCost,
                optimized = true,
                details = mapOf(
                    "indexName" to plan.indexName,
                    "indexedField" to plan.indexedFilter.path,
                    "expectedResults" to plan.expectedResults,
                    "hasRemainingFilter" to (plan.remainingFilter != null)
                )
            )
            is QueryPlan.TextIndexScan -> QueryExplanation(
                planType = "TEXT_INDEX_SCAN",
                indexesUsed = plan.indexedFields.toList(),
                estimatedCost = plan.estimatedCost,
                optimized = true,
                details = mapOf(
                    "indexedFields" to plan.indexedFields,
                    "textField" to plan.textFilter.path,
                    "expectedResults" to plan.expectedResults,
                    "hasRemainingFilter" to (plan.remainingFilter != null)
                )
            )
            is QueryPlan.CompositePlan -> QueryExplanation(
                planType = "COMPOSITE_PLAN",
                indexesUsed = plan.indexScans.map { it.indexName } + 
                             plan.textScans.flatMap { it.indexedFields },
                estimatedCost = plan.estimatedCost,
                optimized = true,
                details = mapOf(
                    "indexScans" to plan.indexScans.size,
                    "textScans" to plan.textScans.size,
                    "expectedResults" to plan.expectedResults,
                    "hasRemainingFilter" to (plan.remainingFilter != null)
                )
            )
        }
    }
}
