package net.barashev.dbi2023

import net.barashev.dbi2023.app.*

/**
 * Ilia Kholkin
 */
class PredicateOptimization(private val storageAccessManager: StorageAccessManager, private val pageCache: PageCache) :
    QueryOptimizer {
    override fun buildPlan(initialPlan: QueryPlan): QueryPlan {
        val statistics = Statistics.managerFactory(storageAccessManager, pageCache)

        val usedFilterIndexes = mutableSetOf<Int>()
        val currentTableNames = mutableSetOf(initialPlan.joinTree[0].leftTable.tableName)
        val currentJoinSize = statistics.getTableStatistics(initialPlan.joinTree[0].leftTable.tableName)?.recordCount

        val resultJoinTree = initialPlan.joinTree.mapIndexed { index, (leftSpec, rightSpec) ->
            //println("join: $leftSpec $rightSpec")
            val newLeftSpec = JoinSpec(leftSpec.tableName, leftSpec.attributeName)
            getBestFilterIndexOrNull(
                currentTableNames,
                initialPlan.filters,
                usedFilterIndexes,
                currentJoinSize,
                statistics
            )?.also { bestReferenceFilterIndex ->
                newLeftSpec.filter = initialPlan.filters[bestReferenceFilterIndex].let {filter ->
                    if (index > 0) filter else
                        FilterSpec(
                            filter.tableName,
                            filter.attributeName,
                            filter.attributeValue,
                            filter.op,
                            getBestAccessMethod(
                                filter,
                                statistics.getTableStatistics(rightSpec.tableName)?.recordCount,
                                statistics
                            )
                        )
                }
                usedFilterIndexes.add(bestReferenceFilterIndex)
            }
            //println("new left=$newLeftSpec")

            val newRightSpec = JoinSpec(rightSpec.tableName, rightSpec.attributeName)
            getBestFilterIndexOrNull(
                setOf(rightSpec.tableName),
                initialPlan.filters,
                usedFilterIndexes,
                statistics.getTableStatistics(rightSpec.tableName)?.recordCount,
                statistics
            )?.also { bestNewFilterIndex ->
                newRightSpec.filter = initialPlan.filters[bestNewFilterIndex].let { filter ->
                    FilterSpec(
                        filter.tableName,
                        filter.attributeName,
                        filter.attributeValue,
                        filter.op,
                        getBestAccessMethod(
                            filter,
                            statistics.getTableStatistics(rightSpec.tableName)?.recordCount,
                            statistics
                        )
                    )
                }
                usedFilterIndexes.add(bestNewFilterIndex)
            }
            //println("new right=$newRightSpec")

            JoinNode(newLeftSpec, newRightSpec)
        }

        val resultFilters = initialPlan.filters.filterIndexed { index, _ -> !usedFilterIndexes.contains(index) }

        return QueryPlan(resultJoinTree, resultFilters)
    }

    private fun getMatchingFilterIndexes(
        tableNames: Set<String>,
        filters: List<FilterSpec>,
        usedFilterIndexes: Set<Int>
    ): List<Int> =
        filters.indices.filter { index ->
            !usedFilterIndexes.contains(index) && tableNames.contains(filters[index].tableName)
        }

    private fun estimateSizeAfterFilter(
        filter: FilterSpec,
        initialSize: Int?,
        attributeStatistics: AttributeStatistics<Comparable<Any>>?
    ): Int = if (attributeStatistics != null) {
        when (filter.op) {
            EQ -> initialSize?.div(attributeStatistics.cardinality) ?: Int.MAX_VALUE

            LT -> attributeStatistics.histogramBuckets.filter { bucket -> bucket.rangeEnd <= filter.attributeValue }
                .sumOf { bucket -> bucket.valueCount }

            GT -> attributeStatistics.histogramBuckets.filter { bucket -> bucket.rangeBegin > filter.attributeValue }
                .sumOf { bucket -> bucket.valueCount }

            LE -> attributeStatistics.histogramBuckets.filter { bucket -> bucket.rangeBegin <= filter.attributeValue }
                .sumOf { bucket -> bucket.valueCount }

            GE -> attributeStatistics.histogramBuckets.filter { bucket -> bucket.rangeEnd > filter.attributeValue }
                .sumOf { bucket -> bucket.valueCount }

            else -> throw IllegalArgumentException("The filter is not supported")
        }
    } else {
        Int.MAX_VALUE
    }

    private fun getBestFilterIndexOrNull(
        tableNames: Set<String>,
        filters: List<FilterSpec>,
        usedFilterIndexes: Set<Int>,
        tableSize: Int?,
        statistics: StatisticsManager
    ): Int? = getMatchingFilterIndexes(tableNames, filters, usedFilterIndexes).minByOrNull { filterIndex ->
        val filter = filters[filterIndex]
        estimateSizeAfterFilter(
            filter,
            tableSize,
            statistics.getAttributeStatistics(
                filter.tableName,
                filter.attributeName
            ) as AttributeStatistics<Comparable<Any>>?
        )
    }

    private fun getBestAccessMethod(
        filter: FilterSpec,
        initialSize: Int?,
        statistics: StatisticsManager
    ): TableAccessMethod {
        val SMALL_TABLE_PAGE_COUNT = 3 // TODO: should it be not just a magic number?
        val MAX_FILTER_RATIO = 0.2

        val attributeStatistics = statistics.getAttributeStatistics(
            filter.tableName,
            filter.attributeName
        ) as AttributeStatistics<Comparable<Any>>?

        if (storageAccessManager.pageCount(filter.tableName) <= SMALL_TABLE_PAGE_COUNT || initialSize == null || attributeStatistics == null) {
            println("using full scan because table is small")
            return TableAccessMethod.FULL_SCAN
        }

        val estimatedSize = estimateSizeAfterFilter(filter, initialSize, attributeStatistics)
        return if (1.0 * estimatedSize / initialSize < MAX_FILTER_RATIO) TableAccessMethod.INDEX_SCAN.also{println("using index scan")} else TableAccessMethod.FULL_SCAN.also {println("usign full scan because of selectivity")}
    }
}
