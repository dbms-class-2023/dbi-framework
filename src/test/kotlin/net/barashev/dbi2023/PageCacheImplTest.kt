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

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

private fun createCache(storage: Storage, maxCacheSize: Int = -1): PageCache = SimplePageCacheImpl(storage, maxCacheSize)

class PageCacheImplTest {
    @Test
    fun `basic test - cache loads pages from the storage and writes them back`() {
        val storage = createHardDriveEmulatorStorage()
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
        val storage = createHardDriveEmulatorStorage()
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
        val storage = createHardDriveEmulatorStorage().also { storage ->
            (1 .. 20).forEach { idx ->
                storage.read(idx).also { page ->
                    page.putRecord(TestRecord(idx, idx).toByteArray())
                    storage.write(page)
                }
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
        val storage = createHardDriveEmulatorStorage()
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

    @Test
    fun `subcache pages eviction priority`() {
        val storage = createHardDriveEmulatorStorage()
        val cache = createCache(storage, maxCacheSize = 10)
        cache.load(1, 5)
        val subcache = cache.createSubCache(5)
        subcache.load(6, 5)
        // +1 hit in main, +1 hit in subcache
        subcache.getAndPin(6)
        assertEquals(1, cache.stats.cacheHit)
        assertEquals(1, subcache.stats.cacheHit)

        // +1 miss in main and subcache
        subcache.getAndPin(20)
        // +1 miss in subcache, +1 hit in main
        subcache.getAndPin(1)
        // +5 hits in main
        (1..5).forEach { cache.getAndPin(it) }
        assertEquals(2, subcache.stats.cacheMiss)
        assertEquals(1, cache.stats.cacheMiss)
        assertEquals(7, cache.stats.cacheHit)
    }

    @Test
    fun `test take drop`() {
        (1..10).toList().let {
            assertEquals(it, it.take(4) + it.drop(4))
        }
    }

}