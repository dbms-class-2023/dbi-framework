package net.barashev.dbi2023

import net.barashev.dbi2023.app.*
import kotlin.math.max
import kotlin.math.min

/**
 * Egor Shibaev
 */
open class GreedyQueryOptimizer(
    accessManager: StorageAccessManager,
    val cache: PageCache,
) : QueryOptimizer {

    protected val stats = Statistics.managerFactory(accessManager, cache)

    protected fun getStats(joinSpec: JoinSpec): AttributeStatistics<Comparable<Any>> =
        stats.buildAttributeStatistics(
            joinSpec.tableName, joinSpec.attributeName,
            attributeTypes[joinSpec.attribute]!!,
            10,
            attributeValueParsers[joinSpec.attribute]!!
        )

    protected fun getSize(joinSpec: JoinSpec): Int {
        val statistics = getStats(joinSpec)
        val filter = { value: Comparable<Any> ->
            joinSpec.filter?.let {
                it.op.test(
                    value.toString().toDouble() as Comparable<Any>,
                    it.attributeValue.toString().toDouble() as Comparable<Any>
                )
            } ?: true
        }
        val estSize = statistics.histogramBuckets.sumOf {
            if (filter(it.rangeBegin))
                it.valueCount
            else
                0
        }
        return estSize
    }

    // rough estimation of Cardinality
    protected fun getCardinality(joinSpec: JoinSpec): Int {
        val statistics = getStats(joinSpec)
        val tableStats = stats.buildTableStatistics(joinSpec.tableName, tableRecordParsers[joinSpec.tableName]!!)
        val sizeWithoutFilter = tableStats.recordCount
        val estSize = getSize(joinSpec)

        val ratioFiltered = estSize.toFloat() / sizeWithoutFilter
        return (ratioFiltered * statistics.cardinality).toInt()
    }

    protected fun estimateJoinedSize(joinNode: JoinNode): Int {
        val sizeLeft = getSize(joinNode.leftTable)
        val cardLeft = getCardinality(joinNode.leftTable)

        val sizeRight = getSize(joinNode.rightTable)
        val cardRight = getCardinality(joinNode.leftTable)

        return sizeLeft * sizeRight / max(max(cardLeft, cardRight), 1)
    }

    protected fun addFilter(joinSpec: JoinSpec, filterSpec: FilterSpec): JoinSpec =
        JoinSpec(joinSpec.tableName, joinSpec.attributeName).also {
            it.filterBy(filterSpec)
        }

    protected fun addFilters(joinSpec: JoinSpec, filterSpecs: List<FilterSpec>): JoinSpec =
        filterSpecs.fold(joinSpec) { acc: JoinSpec, filterSpec: FilterSpec -> addFilter(acc, filterSpec) }

    override fun buildPlan(initialPlan: QueryPlan): QueryPlan {
        val grouped = initialPlan.filters.groupBy { it.tableName }
        val joins = initialPlan.joinTree.map { joinNode ->
            JoinNode(
                addFilters(joinNode.leftTable, grouped[joinNode.leftTable.tableName] ?: emptyList()),
                addFilters(joinNode.rightTable, grouped[joinNode.rightTable.tableName] ?: emptyList()),
                joinNode.joinAlgorithm
            )
        }.toMutableList()

        val firstPair = joins.minBy(::estimateJoinedSize)
        val joinBuilder = JoinBuilder(firstPair)
        joins.remove(firstPair)

        while (joins.isNotEmpty()) {
            val possibleToAdd = joins.filter {
                joinBuilder.tables.contains(it.leftTable.tableName) ||
                    joinBuilder.tables.contains(it.rightTable.tableName)
            }
            assert(possibleToAdd.isNotEmpty())

            val toJoin = possibleToAdd.minBy {
                joinBuilder.getEstimatedJoinedSize(it)
            }

            joinBuilder.add(toJoin)
            joins.removeIf {
                joinBuilder.tables.contains(it.leftTable.tableName) &&
                joinBuilder.tables.contains(it.rightTable.tableName)
            }
        }

        return QueryPlan(joinBuilder.joinTree, emptyList())
    }

    protected inner class JoinBuilder(joinNode: JoinNode) {
        // for attributes
        private var cardinalities: MutableMap<String, Int>

        var tables: HashSet<String>

        private var totalSize = 0

        var joinTree: MutableList<JoinNode>

        // inner should already be in JoinBuilder, outer should not
        fun getEstimatedJoinedSize(joinNode: JoinNode): Int {
            val (innerJoinSpec, outerJoinSpec) = orderInnerOuter(joinNode)
            val newSize = getSize(outerJoinSpec)
            val newCord = getCardinality(outerJoinSpec)

            if (!cardinalities.contains(innerJoinSpec.attributeName)) {
                cardinalities[innerJoinSpec.attributeName] = getCardinality(innerJoinSpec)
            }
            return totalSize * newSize / max(newCord, cardinalities[innerJoinSpec.attributeName]!!)
        }

        init {
            tables = hashSetOf(joinNode.leftTable.tableName, joinNode.rightTable.tableName)

            val cardLeft = getCardinality(joinNode.leftTable)
            val cardRight = getCardinality(joinNode.rightTable)

            totalSize = estimateJoinedSize(joinNode)

            cardinalities = mutableMapOf(
                joinNode.leftTable.attributeName to cardLeft,
                joinNode.rightTable.attributeName to cardRight
            )

            joinTree = mutableListOf(joinNode)
        }

        fun orderInnerOuter(joinNode: JoinNode): JoinNode {
            val firstIn = tables.contains(joinNode.leftTable.tableName)
            val secondIn = tables.contains(joinNode.rightTable.tableName)

            assert(firstIn xor secondIn)

            return if (firstIn) joinNode else JoinNode(
                joinNode.rightTable,
                joinNode.leftTable
            )
        }

        fun add(joinNode: JoinNode) {
            val (innerJoinSpec, outerJoinSpec) = orderInnerOuter(joinNode)

            if (!cardinalities.contains(innerJoinSpec.attributeName)) {
                cardinalities[innerJoinSpec.attributeName] = getCardinality(innerJoinSpec)
            }

            val cardInner = cardinalities[innerJoinSpec.attributeName]!!
            val sizeNew = getSize(outerJoinSpec)
            val cardNew = getCardinality(outerJoinSpec)

            tables.add(outerJoinSpec.tableName)

            totalSize = (totalSize * sizeNew) / max(max(cardInner, cardNew), 1)

            cardinalities[innerJoinSpec.attributeName] = min(cardInner, cardNew)
            cardinalities[outerJoinSpec.attributeName] = min(cardInner, cardNew)

            joinTree.add(JoinNode(innerJoinSpec, outerJoinSpec))
        }

        fun clone(): JoinBuilder {
            val joinBuilder = JoinBuilder(joinTree[0])
            joinBuilder.joinTree = joinTree.toMutableList()
            joinBuilder.tables = tables.toHashSet()
            joinBuilder.cardinalities = cardinalities.toMutableMap()
            joinBuilder.totalSize = totalSize
            return joinBuilder
        }
    }
}