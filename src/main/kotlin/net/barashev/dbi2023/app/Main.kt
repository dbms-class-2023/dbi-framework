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
import net.barashev.dbi2023.createHardDriveEmulatorStorage

fun main(args: Array<String>) = Main().subcommands(SmokeTest(), CacheBenchmark(), SortBenchmark()).main(args)

class Main: CliktCommand() {
    override fun run() = Unit
}

class SmokeTest: CliktCommand() {
    val cacheSize: Int by option(help="Page cache size [default=100]").int().default(System.getProperty("cache.size", "100").toInt())
    val cacheImpl: String by option(help="Cache implementation [default=fifo]").default(System.getProperty("cache.impl", "fifo"));
    val sortImpl: String by option(help="Merge sort implementation [default=fake]").default(System.getProperty("sort.impl", "fake"))

    val dataScale: Int by option(help="Test data scale [default=1]").int().default(1)
    val randomDataSize by option(help="Shall the generated data amount be random [default=false]").flag(default = false)
    val joinClause: String by option(help="JOIN clause, e.g. 'planet.id:flight.planet_id'").default("")
    val filterClause: String by option(help="Filter clause, e.g. 'planet.id = 1'").default("")
    val indexClause: String by option(help="Index clause to create indexes before running a query, e.g. flight.num").default("")
    val disableStatistics by option(help="Disable collection of attribute statistics [default=false]").flag(default = false)

    override fun run() {
        val storage = createHardDriveEmulatorStorage()
        val (cache, accessManager) = initializeFactories(storage, cacheSize, cacheImpl, sortImpl)
        DataGenerator(accessManager, cache, dataScale, !randomDataSize, disableStatistics).use{}

        val populateCost = storage.totalAccessCost
        println("The cost to populate tables: ${populateCost}")
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
        println("Total cost: ${storage.totalAccessCost}. Scan cost: ${storage.totalAccessCost - populateCost}")
    }
}



