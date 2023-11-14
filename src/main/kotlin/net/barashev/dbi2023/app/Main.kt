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

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import net.barashev.dbi2023.*

fun main(args: Array<String>) = Main().subcommands(SmokeTest(), CacheBenchmark(), SortBenchmark(), HashBenchmark(), JoinBenchmark()).main(args)

class Main: CliktCommand() {
    override fun run() = Unit
}

class SmokeTest: CliktCommand() {
    val cacheSize: Int by option(help="Page cache size [default=100]").int().default(System.getProperty("cache.size", "100").toInt())
    val cacheImpl: String by option(help="Cache implementation [default=fifo]").default(System.getProperty("cache.impl", "fifo"));
    val sortImpl: String by option(help="Merge sort implementation [default=fake]").default(System.getProperty("sort.impl", "fake"))
    val indexImpl: String by option(help="Indexes implementation [default=fake]").default(System.getProperty("index.impl", "fake"))
    val optimizerImpl: String by option(help="Optimizer implementation [default=fake]").default(System.getProperty("optimizer.impl", "fake"))

    val dataScale: Int by option(help="Test data scale [default=1]").int().default(1)
    val randomDataSize by option(help="Shall the generated data amount be random [default=false]").flag(default = false)
    val joinClause: String by option(help="JOIN clause, e.g. 'planet.id:flight.planet_id'").default("")
    val filterClause: String by option(help="Filter clause, e.g. 'planet.id = 1'").default("")
    val indexClause: String by option(help="Index clause to create indexes before running a query, e.g. flight.num").default("")
    val disableStatistics by option(help="Disable collection of attribute statistics [default=false]").flag(default = false)

    override fun run() {
        val storage = createHardDriveEmulatorStorage()
        val (cache, accessManager) = initializeFactories(storage, cacheSize, cacheImpl, sortImpl, indexImpl, optimizerImpl)
        DataGenerator(accessManager, cache, dataScale, !randomDataSize, disableStatistics).use {}

        val populateCost = storage.totalAccessCost
        println("The cost to populate tables: ${populateCost}")

        accessManager.buildIndexes(parseIndexClause(indexClause))

        val costAfterIndexes = storage.totalAccessCost
        println("Access cost after creation of tables and indexes: $costAfterIndexes")

        if (joinClause.isNotBlank() || filterClause.isNotBlank()) {
            executeQueryPlan(accessManager, cache, storage)
        } else {
            printTableContents(accessManager, storage)
        }
    }

    private fun executeQueryPlan(accessManager: StorageAccessManager, cache: PageCache, storage: Storage) {
        val cost0 = storage.totalAccessCost
        val innerJoins = parseJoinClause(joinClause)
        val filters = parseFilterClause(filterClause)
        val plan = Optimizer.factory(accessManager, cache).buildPlan(QueryPlan(innerJoins, filters))
        println("We're executing the following query plan: $plan")

        var rowCount = 0
        val joinResult = QueryExecutor(accessManager, cache, tableRecordParsers, attributeValueParsers).run {
            execute(plan)
        }
        val joinedTables = joinResult.realTables
        accessManager.createFullScan(joinResult.tableName).records { bytes ->
            parseJoinedRecord(bytes, joinedTables, tableRecordParsers)
        }.forEach {
            rowCount++
            it.entries.forEach { (tableName, recordBytes) ->
                println("$tableName: ${tableRecordParsers[tableName]!!.invoke(recordBytes)}")
            }
            println("----")
        }
        println("Execution cost: ${storage.totalAccessCost - cost0}")
    }

    fun printTableContents(accessManager: StorageAccessManager, storage: Storage) {
        val cost0 = storage.totalAccessCost
        println("Now we will print the contents of all tables")

        println("Planet table:")
        accessManager.createFullScan("planet").records { planetRecord(it) }.forEach { println(it) }

        println()
        println("Spacecraft table:")
        accessManager.createFullScan("spacecraft").records { spacecraftRecord(it) }.forEach { println(it) }

        println()
        println("Flight table:")
        accessManager.createFullScan("flight").records { flightRecord(it) }.forEach { println(it) }

        println()
        println("Ticket table:")
        accessManager.createFullScan("ticket").records { ticketRecord(it) }.forEach { println(it) }
        println("Total cost: ${storage.totalAccessCost}. Scan cost: ${storage.totalAccessCost - cost0}")
    }
}

fun StorageAccessManager.buildIndexes(specs: List<IndexSpec>) {
    specs.forEach { spec ->
        val attributeType = attributeTypes["${spec.tableName}.${spec.attributeName}"] ?: throw IllegalStateException("Can't find attribute type for $spec")
        val attributeValueParser = attributeValueParsers["${spec.tableName}.${spec.attributeName}"] ?: throw IllegalStateException("Can't find attribute value parser for $spec")
        println("Creating index $spec")
        createIndex(spec.tableName, spec.attributeName, attributeType, attributeValueParser)
    }
}

