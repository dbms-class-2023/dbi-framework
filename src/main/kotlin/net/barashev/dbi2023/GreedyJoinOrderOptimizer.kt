package net.barashev.dbi2023

import net.barashev.dbi2023.app.FilterSpec
import net.barashev.dbi2023.app.JoinSpec
import kotlin.math.max

/**
 * Nodes of the graph are JoinSpec values. An edge between two nodes signifies that the two nodes
 * can be joined.
 *
 * The graph is built from the initial query plan by filling it with transitive edges.
 *
 * Veronika Sirotkina
 */
class JoinGraph(joinTree: JoinTree) {
    data class Edge(
        val from: Node,
        val to: Node,
    ) {
        constructor(joinNode: JoinNode) : this(Node(joinNode.leftTable), Node(joinNode.rightTable))
    }

    data class Node(val tableName: String, val attributeName: String, val filter: FilterSpec?) {
        constructor(joinSpec: JoinSpec) : this(joinSpec.tableName, joinSpec.attributeName, joinSpec.filter)
    }

    val edges: Map<Node, Set<Edge>>
    val nodes
        get() = edges.keys
    val tableNameToNodesMapping: Map<String, List<Node>>

    init {
        val nodes = joinTree.flatMap { join -> listOf(Node(join.leftTable), Node(join.rightTable)) }
        val nodeMapping = nodes.associateWith { mutableSetOf<Edge>() }
        // Fill the graph with the initial set of possible join edges
        joinTree.forEach { joinNode ->
            val node1 = Node(joinNode.leftTable)
            val node2 = Node(joinNode.rightTable)
            nodeMapping[node1]!!.add(Edge(node1, node2))
            nodeMapping[node2]!!.add(Edge(node2, node1))
        }
        // Add transitive edges. In each iteration for each two adjacent edges
        // a new transitive edge is added. A path spans at most `nodes.size` nodes,
        // so it will definitely be covered by a transitive edge after `nodes.size - 1` iterations.
        // (there is a stronger estimation but this one is enough to prove polynomial complexity)
        for (i in 1 until nodes.size) {
            var edgesAdded = false
            for (edgeSet in nodeMapping.values) {
                for (edge1 in edgeSet) {
                    for (edge2 in edgeSet) {
                        if (edge1 == edge2) {
                            continue
                        }
                        nodeMapping[edge1.to]!!.add(Edge(edge1.to, edge2.to))
                        nodeMapping[edge2.to]!!.add(Edge(edge2.to, edge1.to))
                        edgesAdded = true
                    }
                }
            }
            if (!edgesAdded) {
                break
            }
        }
        edges = nodeMapping
        tableNameToNodesMapping = this.nodes.groupBy { it.tableName }
    }

    fun getAdjacentEdges(node: JoinSpec) = getAdjacentEdges(Node(node))
    fun getAdjacentEdges(node: Node) = edges[node]!!
    fun getTableAdjacentEdges(tableName: String) =
        tableNameToNodesMapping.getOrDefault(tableName, emptyList())
            .flatMap { getAdjacentEdges(it) }
}

class GreedyJoinOrderOptimizer(
    private val statisticsManager: StatisticsManager
) : QueryOptimizer {

    private data class CostFunctionData(
        val minCost: Double, val whenJoinedWith: JoinGraph.Node?
    )

    private fun JoinGraph.Node.toJoinSpec() = JoinSpec(tableName, attributeName).also { spec -> this.filter?.let { spec.filterBy(it) }}

    override fun buildPlan(initialPlan: QueryPlan): QueryPlan {
        val joinGraph = JoinGraph(initialPlan.joinTree)

        // Find a pair of JoinSpecs that can be joined with min cost.
        var firstJoin = JoinGraph.Edge(initialPlan.joinTree.first())
        var minJoinSize = joinSizeEstimate(firstJoin)
        for (node in joinGraph.nodes) {
            for (edge in joinGraph.getAdjacentEdges(node)) {
                val joinSize = joinSizeEstimate(edge)
                if (joinSize < minJoinSize) {
                    minJoinSize = joinSize
                    firstJoin = edge
                }
            }
        }
        val newQueryPlan = mutableListOf(firstJoin)

        // A list of nodes that still need to be joined and the estimated min cost of joining them
        val joinCost = joinGraph.nodes.filterNot { node -> node == firstJoin.from || node == firstJoin.to }
            .associateWith { CostFunctionData(Double.MAX_VALUE, null) }
            .toMutableMap()

        val updateJoinCost = { tableName: String ->
            joinGraph.getTableAdjacentEdges(tableName).forEach { edge ->
                joinCost[edge.to]?.let { (nodeJoinCost, _) ->
                    val estimate = joinRelativeSizeEstimate(edge)
                    if (estimate < nodeJoinCost) {
                        joinCost[edge.to] = CostFunctionData(estimate, edge.from)
                    }
                }
            }
        }

        updateJoinCost(firstJoin.from.tableName)
        updateJoinCost(firstJoin.to.tableName)
        var estimatedIntermediateSize = minJoinSize
        while (joinCost.isNotEmpty()) {
            val nextJoinNode = joinCost.minBy { (_, costFunctionData) -> costFunctionData.minCost }
                .let { (newNode, costFunctionData) -> JoinGraph.Edge(costFunctionData.whenJoinedWith!!, newNode) }
            updateJoinCost(nextJoinNode.to.tableName)
            joinCost.remove(nextJoinNode.from)
            joinCost.remove(nextJoinNode.to)
            newQueryPlan.add(nextJoinNode)
            estimatedIntermediateSize = joinSizeEstimate(nextJoinNode, estimatedIntermediateSize)
        }
        return QueryPlan(newQueryPlan.map { edge -> JoinNode(edge.from.toJoinSpec(), edge.to.toJoinSpec()) }, initialPlan.filters)
    }

    private fun joinSizeEstimate(
        edge: JoinGraph.Edge,
        leftTableSizeEstimate: Double? = null
    ): Double {
        val leftTable = edge.from
        val rightTable = edge.to
        val leftTableRecordSize = leftTableSizeEstimate
                                  ?: statisticsManager.getTableStatistics(leftTable.tableName)!!.recordCount.toDouble()
        val rightTableRecordSize = statisticsManager.getTableStatistics(rightTable.tableName)!!.recordCount
        val leftCardinality = statisticsManager.getAttributeStatistics(leftTable.tableName, leftTable.attributeName)!!.cardinality
        val rightCardinality = statisticsManager.getAttributeStatistics(rightTable.tableName, rightTable.attributeName)!!.cardinality
        return leftTableRecordSize * rightTableRecordSize / max(leftCardinality, rightCardinality)
    }

    /**
     * The next cheapest join is picked based on this function.
     * `leftTableRecordSize` is not used in this formula because it corresponds to the
     * estimated size of an intermediate join. In each iteration of the algorithm
     * `leftTableRecordSize` is the same no matter which join is picked next, so
     * it doesn't change which join is estimated to be the cheapest.
     */
    private fun joinRelativeSizeEstimate(
        edge: JoinGraph.Edge,
    ): Double {
        val leftTable = edge.from
        val rightTable = edge.to
        val rightTableRecordSize = statisticsManager.getTableStatistics(rightTable.tableName)!!.recordCount
        val leftCardinality = statisticsManager.getAttributeStatistics(leftTable.tableName, leftTable.attributeName)!!.cardinality
        val rightCardinality = statisticsManager.getAttributeStatistics(rightTable.tableName, rightTable.attributeName)!!.cardinality
        return rightTableRecordSize.toDouble() / max(leftCardinality, rightCardinality)
    }
}
