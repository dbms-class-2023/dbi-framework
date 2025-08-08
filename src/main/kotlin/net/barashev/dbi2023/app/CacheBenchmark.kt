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
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.double
import com.github.ajalt.clikt.parameters.types.int
import net.barashev.dbi2023.CachedPage
import net.barashev.dbi2023.PageId
import net.barashev.dbi2023.createHardDriveEmulatorStorage
import org.slf4j.LoggerFactory
import java.util.*
import kotlin.math.max
import kotlin.math.min

class CacheBenchmark: CliktCommand() {
    val dataScale: Int by option(help="Test data scale [default=1]").int().default(1)
    val cacheSize: Int by option(help="Page cache size [default=100]").int().default(System.getProperty("cache.size", "100").toInt())
    val cacheImpl: String by option(help="Cache implementation [default=fifo]").default(System.getProperty("cache.impl", "fifo"));
    val testFullScanChance: Double by option(help="The probability of a full scan of flight table [default=${(1/30.0)}]").double().default(1/30.0)
    val testLongPinChance: Double by option(help="The probability of a full scan of flight table [default=${(1/3.0)}]").double().default(1/3.0)
    override fun run() {
        val storage = createHardDriveEmulatorStorage()
        val (cache, accessManager) = initializeFactories(storage, storage, cacheSize, cacheImpl)
        DataGenerator(accessManager, cache, dataScale, fixedRowCount = true, disableStatistics = true).use{}

        val planetPages = accessManager.pageCount("planet")
        val spacecraftPages = accessManager.pageCount("spacecraft")
        val flightPageCount = accessManager.pageCount("flight")
        val ticketPageCount = accessManager.pageCount("ticket")

        val ticketPages = accessManager.createFullScan("ticket").pages().map { it.close(); it.id }.toList()
        val flightPages = accessManager.createFullScan("flight").pages().map { it.close(); it.id }.toList()
        val pageWorkingSet = (ticketPages + flightPages).shuffled().toList()

        LOG.info("Page count: planet=$planetPages spacecraft=$spacecraftPages flight=$flightPageCount ticket=$ticketPageCount")
        LOG.info(cache.stats.toString())
        cache.stats.reset()
        cache.flush()

        val accessCost1 = storage.totalAccessCost
        val random = Random().also { it.setSeed(System.currentTimeMillis()) }
        var fullScanCount = 0
        var longPinCount = 0
        var maxLongPinCount = 0

        val pageAccessCounts = mutableMapOf<PageId, Int>()
        val pinnedPages = mutableListOf<CachedPage>()
        repeat(2000) {
            // With some probability we will do a full scan of a pretty large table, that will possible replace many
            // pages in the cache.
            if (random.nextDouble() < testFullScanChance) {
                LOG.debug("---- full scan! ----")
                accessManager.createFullScan("ticket").pages().forEach { it.close() }
                fullScanCount += 1
                LOG.debug("---- end of full scan! ----")
            }
            // Sometimes we will close some of the long-pinned pages
            if (pinnedPages.isNotEmpty() && random.nextInt(5) == 1) {
                val donePages = pinnedPages.subList(0, random.nextInt(pinnedPages.size))
                donePages.forEach { it.close() }
                donePages.clear()
            }

            val pageNumber = if (random.nextBoolean()) {
                // Now generate a random page to fetch.
                random.nextGaussian(pageWorkingSet.size / 2.0, pageWorkingSet.size / 4.0).toInt()
                    .let { max(it, 0) }.let { min(it, pageWorkingSet.size - 1) }
            } else {
                random.nextInt(0, pageWorkingSet.size).toInt()
                    .let { max(it, 0) }.let { min(it, pageWorkingSet.size - 1) }
            }


            val pageId = pageWorkingSet[pageNumber]
            pageAccessCounts.getOrPut(pageId) { 0 }
            pageAccessCounts[pageId] = pageAccessCounts[pageId]!! + 1

            val page = cache.getAndPin(pageWorkingSet[pageNumber])
            // With some probability we will "long pin" the page, as if there is a relatively long transaction
            // that modifies the page.
            if (pinnedPages.size < cacheSize - 1 && random.nextDouble() < testLongPinChance) {
                if (pinnedPages.firstOrNull { it.id == pageId } == null) {
                    pinnedPages.add(page)
                    longPinCount += 1
                    maxLongPinCount = max(maxLongPinCount, pinnedPages.size)
                }
            } else {
                page.close()
            }
        }
        pinnedPages.forEach { it.close() }
        LOG.debug("page access: ${pageAccessCounts.entries.sortedBy { it.value }.reversed()}")
        LOG.debug("totally we requested ${pageAccessCounts.size} random pages")
        LOG.info("Access cost: ${storage.totalAccessCost - accessCost1}")
        LOG.info(cache.stats.toString())
        LOG.info("Test stats: #full scans=$fullScanCount #long pins=$longPinCount, max long pin count=$maxLongPinCount")
    }
}

private val LOG = LoggerFactory.getLogger("Cache.Benchmark")