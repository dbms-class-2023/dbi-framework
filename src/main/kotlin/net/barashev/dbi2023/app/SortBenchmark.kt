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
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import net.barashev.dbi2023.Operations
import net.barashev.dbi2023.createHardDriveEmulatorStorage
import net.barashev.dbi2023.fake.FakeMergeSort

class SortBenchmark: CliktCommand() {
    val dataScale: Int by option(help="Test data scale [default=1]").int().default(1)
    val cacheSize: Int by option(help="Page cache size [default=100]").int().default(System.getProperty("cache.size", "100").toInt())
    val cacheImpl: String by option(help="Cache implementation [default=fifo]").default(System.getProperty("cache.impl", "fifo"));
    val realSort by option(help="Use the real multiway merge sort implementation [default=false]").flag()

    override fun run() {
        val storage = createHardDriveEmulatorStorage()
        val (cache, accessManager) = initializeFactories(storage = storage, cacheSize = cacheSize,
            cacheImpl = cacheImpl,
            sortImpl = if (realSort) "real" else "fake"
        )
        DataGenerator(accessManager, cache, dataScale, fixedRowCount = true, disableStatistics = true).use{}

        val planetPages = accessManager.pageCount("planet")
        val spacecraftPages = accessManager.pageCount("spacecraft")
        val flightPageCount = accessManager.pageCount("flight")
        val ticketPageCount = accessManager.pageCount("ticket")

        val ticketPages = accessManager.createFullScan("ticket").pages().map { it.close(); it.id }.toList()

        println("Page count: planet=$planetPages spacecraft=$spacecraftPages flight=$flightPageCount ticket=$ticketPageCount")
        println(cache.stats)
        cache.stats.reset()

        val cost0 = storage.totalAccessCost
        val sortedTicketsTable = Operations.sortFactory(accessManager, cache).sort("ticket") {
            ticketRecord(it).value3
        }
        val cost1 = storage.totalAccessCost
        println("Sorting done. Access cost=${cost1-cost0}")
        println(cache.stats)
        val realSortMin = accessManager.createFullScan(sortedTicketsTable).records(::ticketRecord).first()

        accessManager.deleteTable(sortedTicketsTable)
        val fakeSortedTicketsTable = FakeMergeSort(accessManager, cache).sort("ticket") { ticketRecord(it).value3 }
        val fakeSortMin = accessManager.createFullScan(fakeSortedTicketsTable).records(::ticketRecord).first()

        println("The cheapest ticket found by the real merge sort: $realSortMin")
        println("The cheapest ticket found by the fake merge sort: $fakeSortMin")
        if (realSortMin.value3 != fakeSortMin.value3) {
            error("Wow, they found different min ticket prices!!!")
        }
    }
}