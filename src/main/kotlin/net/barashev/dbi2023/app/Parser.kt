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
package net.barashev.dbi2023.app

import net.barashev.dbi2023.JoinNode
import net.barashev.dbi2023.TableAccessMethod
import java.util.function.BiPredicate

data class FilterSpec(
    val tableName: String, val attributeName: String, val attributeValue: Comparable<Any>,
    val op: BiPredicate<Comparable<Any>, Comparable<Any>>,
    val accessMethod: TableAccessMethod = TableAccessMethod.FULL_SCAN) {
    val attribute get() =
        if (attributeName.indexOf('.') >= 0) {
            attributeName
        } else "$tableName.$attributeName"

    override fun toString(): String {
        return "$accessMethod[$attribute ${op.asText()} $attributeValue]"
    }
}

private fun BiPredicate<Comparable<Any>, Comparable<Any>>.asText(): String =
    when (this) {
        EQ -> "="
        LT -> "<"
        GT -> ">"
        LE -> "<="
        GE -> ">="
        else -> "?"
    }

class JoinSpec(val tableName: String, val attributeName: String) {
    val attribute get() =
        if (attributeName.indexOf('.') >= 0) {
            attributeName
        } else {
            "${realTables.joinToString(separator = ",")}.${attributeName}"
        }

    var filter: FilterSpec? = null


    operator fun component1() = tableName
    operator fun component2() = attribute
    override fun toString(): String {
        return "JoinSpec(tableName='$tableName', attributeShortName='$attributeName')".let {join ->
            filter?.let {
                "$join[$it]"
            } ?: join
        }
    }

    fun filterBy(filterSpec: FilterSpec) {
        this.filter = filterSpec
    }
    val realTables: List<String> = tableName.split(",").filter { !it.startsWith("@") }
}


fun parseJoinClause(joinClause: String): List<JoinNode> =
    if (joinClause.isBlank()) emptyList()
    else
        joinClause.split("""\s+""".toRegex()).map { pair ->
            val (left, right) = pair.split(":", limit = 2)
            val parseSpec = { stringSpec: String ->
                val (table, attribute) = stringSpec.split(".", limit = 2)
                JoinSpec(table, attribute)
            }
            JoinNode(parseSpec(left), parseSpec(right))
        }.toList()

val EQ: BiPredicate<Comparable<Any>, Comparable<Any>> = BiPredicate { left, right -> left == right }
val GT: BiPredicate<Comparable<Any>, Comparable<Any>> = BiPredicate { left, right -> left > right }
val LT: BiPredicate<Comparable<Any>, Comparable<Any>> = BiPredicate { left, right -> left < right }
val LE: BiPredicate<Comparable<Any>, Comparable<Any>> = BiPredicate { left, right -> left <= right }
val GE: BiPredicate<Comparable<Any>, Comparable<Any>> = BiPredicate { left, right -> left >= right }

fun parseFilterClause(filterClause: String): List<FilterSpec> =
    if (filterClause.isBlank()) emptyList()
    else
        filterClause.split("""&+""".toRegex()).map { condition ->
            val (attributeFullName, predicate, literal) = condition.trim().split("""\s+""".toRegex(), limit = 3)
            val (table, attribute) = attributeFullName.split(".", limit = 2)
            val literalValue = (attributeTypes["$table.$attribute"])?.defaultValue()?.let {
                when {
                    (it as? Int) != null -> literal.trim().toInt()
                    (it as? Double) != null -> literal.trim().toDouble()
                    (it as? Boolean) != null -> literal.trim().toBoolean()
                    else -> null
                }
            } ?: error("Unknown attribute ${table}.${attribute}")
            FilterSpec(table.trim(), attribute.trim(), literalValue as Comparable<Any>,
                when (predicate.trim()) {
                    "=" -> EQ
                    ">" -> GT
                    "<" -> LT
                    "<=" -> LE
                    ">=" -> GE
                    else -> error("Unsupported predicate $predicate")
                }
            )
        }.toList()

data class IndexSpec(val tableName: String, val attributeName: String)
fun parseIndexClause(indexClause: String): List<IndexSpec> =
    if (indexClause.isBlank()) emptyList()
    else
        indexClause.split(",").map { indexSpec ->
            val (table, attribute) = indexSpec.trim().split(".", limit = 2)
            IndexSpec(table, attribute)
        }.toList()

