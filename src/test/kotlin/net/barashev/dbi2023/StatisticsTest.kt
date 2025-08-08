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

import net.barashev.dbi2023.app.initializeFactories
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class StatisticsTest {
    private lateinit var storage: Storage
    @BeforeEach
    fun initialize() {
        storage = createHardDriveEmulatorStorage()
        initializeFactories(storage)
    }

    @Test
    fun `unique field, basic test`() {
        val cache = FifoPageCacheImpl(storage, 20)
        val accessMethodManager = SimpleStorageAccessManager(cache, storage)

        val table1 = accessMethodManager.createTable("table1")
        TableBuilder(accessMethodManager, cache, table1).use {builder ->
            (1..100).forEach { builder.insert(Record1(intField(it)).asBytes()) }
        }
        val statisticsManager = Statistics.managerFactory(accessMethodManager, cache)
        val fooStats = statisticsManager.buildAttributeStatistics("table1", "foo", IntAttribute(), 10) {
            Record1(intField()).fromBytes(it).value1
        }
        assertEquals(100, fooStats.cardinality)
        fooStats.histogramBuckets.forEachIndexed{ idx, bucket ->
            assertEquals(10, bucket.valueCount)
            assertEquals(idx*10 + 1, bucket.rangeBegin)
            assertEquals(idx*10 + 10, bucket.rangeEnd)
        }
    }

    @Test
    fun `field with duplicates, basic test`() {
        val cache = FifoPageCacheImpl(storage, 20)
        val accessMethodManager = SimpleStorageAccessManager(cache, storage)

        val table1 = accessMethodManager.createTable("table1")
        TableBuilder(accessMethodManager, cache, table1).use {builder ->
            (1..50).forEach { builder.insert(Record1(intField(it)).asBytes()) }
            (1..50).forEach { builder.insert(Record1(intField(it)).asBytes()) }
        }
        val statisticsManager = Statistics.managerFactory(accessMethodManager, cache)
        val fooStats = statisticsManager.buildAttributeStatistics("table1", "foo", IntAttribute(), 10) {
            Record1(intField()).fromBytes(it).value1
        }
        assertEquals(50, fooStats.cardinality)
        fooStats.histogramBuckets.forEachIndexed{ idx, bucket ->
            assertEquals(10, bucket.valueCount)
            assertEquals(idx*5 + 1, bucket.rangeBegin)
            assertEquals(idx*5 + 5, bucket.rangeEnd)
        }
    }

    @Test
    fun `field with a single value`() {
        val cache = FifoPageCacheImpl(storage, 20)
        val accessMethodManager = SimpleStorageAccessManager(cache, storage)

        val table1 = accessMethodManager.createTable("table1")
        TableBuilder(accessMethodManager, cache, table1).use {builder ->
            (1..100).forEach { builder.insert(Record1(intField(1)).asBytes()) }
        }
        val statisticsManager = Statistics.managerFactory(accessMethodManager, cache)
        val fooStats = statisticsManager.buildAttributeStatistics("table1", "foo", IntAttribute(), 10) {
            Record1(intField()).fromBytes(it).value1
        }
        assertEquals(1, fooStats.cardinality)
        assertEquals(1, fooStats.histogramBuckets.size)
        assertEquals(100, fooStats.histogramBuckets[0].valueCount)
        assertEquals(1, fooStats.histogramBuckets[0].rangeBegin)
        assertEquals(1, fooStats.histogramBuckets[0].rangeEnd)
    }

    @Test
    fun `table record count`() {
        val cache = FifoPageCacheImpl(storage, 20)
        val accessMethodManager = SimpleStorageAccessManager(cache, storage)

        val table1 = accessMethodManager.createTable("table1")
        TableBuilder(accessMethodManager, cache, table1).use {builder ->
            (1..100).forEach { builder.insert(Record1(intField(it)).asBytes()) }
        }
        val statisticsManager = Statistics.managerFactory(accessMethodManager, cache)
        val tableStats = statisticsManager.buildTableStatistics("table1") {
            Record1(intField()).fromBytes(it)
        }

        assertEquals(100, tableStats.recordCount)
    }


}