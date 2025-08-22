/*
 * Copyright 2023 Dmitry Barashev, JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package net.barashev.dbi2023

import net.barashev.dbi2023.app.FilterSpec
import net.barashev.dbi2023.app.JoinSpec


/**
 * Query optimizer transforms an initial query plan into equivalent plan which is likely to be more efficient.
 * The input is a query plan which consists of a left-recursive join tree and a list of filters.
 * Technically a join tree is a list of join pairs, and the order of join pairs in the list corresponds
 * to the order in which they will be joined. Thus, one of the optimizer tasks is to find an optimal permutation of the
 * join pairs.
 * Filters will be applied to the whole join result, if specified directly in QueryPlan::filters property.
 * They can also be pushed down into joins using JoinSpec::filter property.
 * Thus, another optimizer task is to decide whether some filter needs to be pushed down or not.
 *
 * Technical subtleties:
 * - except for the first pair in the join list, the left component of each pair must refer to one of the tables already
 *   used in the list prefix.
 *   For instance, this plan is ok: planet.id:flight.planet_id flight.spacecraft_id:spacecraft.id
 *   This plan, where the second pair operands are swapped,  is not ok: planet.id:flight.planet_id spacecraft.id:flight.spacecraft_id
 * A query executor materializes all intermediate results (that is, writes them to the "disk storage")
 */
interface QueryOptimizer {
    fun buildPlan(initialPlan: QueryPlan): QueryPlan
}

enum class TableAccessMethod {
    FULL_SCAN, INDEX_SCAN
}
data class JoinNode(
    val leftTable: JoinSpec, val rightTable: JoinSpec,
    var joinAlgorithm: JoinAlgorithm = JoinAlgorithm.NESTED_LOOPS
)

typealias JoinTree = List<JoinNode>
data class QueryPlan(val joinTree: JoinTree, val filters: List<FilterSpec>)

object Optimizer {
    var factory: (StorageAccessManager, PageCache) -> QueryOptimizer = { _, _ ->
        TODO("Please DO NOT WRITE your code here. Set the factory instance where you need it: Optimizer.factory = ")
    }
}

class VoidOptimizer : QueryOptimizer {
    override fun buildPlan(initialPlan: QueryPlan): QueryPlan = initialPlan
}
