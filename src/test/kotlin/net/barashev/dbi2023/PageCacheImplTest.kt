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
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

private fun createCache(storage: Storage, maxCacheSize: Int = -1): PageCache = CacheManager.factory(storage, maxCacheSize)

class PageCacheImplTest {
    var storage = createHardDriveEmulatorStorage()
    @BeforeEach
    fun setupAll() {
        storage = createHardDriveEmulatorStorage()
        initializeFactories(storage)
    }
    @Test
    fun `basic test - cache loads pages from the storage and writes them back`() {
        val cache = createCache(storage)
        val pageId = storage.read(1).also { page ->
            page.putRecord(TestRecord(1,1).toByteArray(), 0)
            storage.write(page)
        }.id

        val cost = storage.totalAccessCost
        cache.getAndPin(pageId).use {
            assertEquals(TestRecord(1,1), TestRecord.fromByteArray(it.getRecord(0).bytes))
            it.putRecord(TestRecord(2,2).toByteArray())
        }
        cache.flush()
        assertEquals(TestRecord(2,2), TestRecord.fromByteArray(storage.read(pageId).getRecord(1).bytes))
        assertTrue(storage.totalAccessCost > cost)
    }

    @Test
    fun `pin after load costs zero`() {
        val cache = createCache(storage)
        val pageId = storage.read(1).also { page ->
            page.putRecord(TestRecord(1,1).toByteArray(), 0)
            storage.write(page)
        }.id
        cache.load(pageId)
        val cost = storage.totalAccessCost

        cache.getAndPin(pageId)
        assertEquals(0.0, storage.totalAccessCost - cost)
    }

    @Test
    fun `sequential load costs less than random gets`() {
        (1 .. 20).forEach { idx ->
            storage.read(idx).also { page ->
                page.putRecord(TestRecord(idx, idx).toByteArray())
                storage.write(page)
            }
        }
        val cache = createCache(storage)
        val cost1 = storage.totalAccessCost
        val coldPages = (1 .. 10).map { idx -> cache.getAndPin(idx) }.toList()
        val cost2 = storage.totalAccessCost
        cache.load(11, 10)
        val warmPages = (11 .. 20).map { idx -> cache.getAndPin(idx) }.toList()
        val cost3 = storage.totalAccessCost
        assertTrue(cost2 - cost1 > cost3 - cost2)
    }

    @Test
    fun `pages are evicted when cache is full`() {
        val cache = createCache(storage, maxCacheSize = 5)
        cache.load(1, 5)
        val cost1 = storage.totalAccessCost

        cache.getAndPin(10).close()
        val cost2 = storage.totalAccessCost
        assertEquals(1, cache.stats.cacheMiss)

        (5 downTo 1).forEach { cache.getAndPin(it) }
        val cost3 = storage.totalAccessCost
        assertEquals(4, cache.stats.cacheHit)
        assertEquals(2, cache.stats.cacheMiss)

        assertTrue(cost3 > cost2)
    }
}