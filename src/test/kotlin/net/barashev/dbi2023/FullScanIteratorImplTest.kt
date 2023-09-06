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

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class FullScanIteratorImplTest {
    private fun createRootRecords(cache: PageCache) {
        cache.getAndPin(0).use {buf ->
            buf.putRecord(OidPageidRecord(intField(NAME_SYSTABLE_OID), intField(1000)).asBytes())
        }

    }

    @Test
    fun `test table is missing`() {
        val storage = createHardDriveEmulatorStorage()
        val cache = SimplePageCacheImpl(storage, 20)
        val rootRecords = RootRecords(cache, 0, 1)
        assertTrue(FullScanImpl(cache, NAME_SYSTABLE_OID, rootRecords::iterator).records {
            error("Not expected to be here")
        }.toList().isEmpty())
    }

    @Test
    fun `test table is empty`() {
        val storage = createHardDriveEmulatorStorage()
        val cache = SimplePageCacheImpl(storage, 20)
        createRootRecords(cache)
        val rootRecords = RootRecords(cache, 0, 1)
        assertTrue(FullScanImpl(cache, NAME_SYSTABLE_OID, rootRecords::iterator).records {
            error("Not expected to be here")
        }.toList().isEmpty())
    }

    @Test
    fun `test single page table`() {
        val storage = createHardDriveEmulatorStorage()
        val cache = SimplePageCacheImpl(storage, 20)
        createRootRecords(cache)
        val rootRecords = RootRecords(cache, 0, 1)
        cache.getAndPin(1000).let {
            it.putRecord(OidNameRecord(intField(2), stringField("table2"), booleanField(false)).asBytes())
            it.putRecord(OidNameRecord(intField(3), stringField("table3"), booleanField(false)).asBytes())
        }
        assertEquals(
            listOf("table2", "table3"),
            FullScanImpl(cache, NAME_SYSTABLE_OID, rootRecords::iterator).records {
                OidNameRecord(intField(), stringField(), booleanField(false)).fromBytes(it)
            }.map { it.value2 }.toList()
        )
    }

    @Test
    fun `test multiple page table`() {
        val storage = createHardDriveEmulatorStorage()
        val cache = SimplePageCacheImpl(storage, 20)
        cache.getAndPin(0).use {buf ->
            buf.putRecord(OidPageidRecord(intField(NAME_SYSTABLE_OID), intField(1000)).asBytes())
            buf.putRecord(OidPageidRecord(intField(NAME_SYSTABLE_OID), intField(1001)).asBytes())
        }
        val rootRecords = RootRecords(cache, 0, 1)
        cache.getAndPin(1000).let {
            it.putRecord(OidNameRecord(intField(2), stringField("table2"), booleanField(false)).asBytes())
            it.putRecord(OidNameRecord(intField(3), stringField("table3"), booleanField(false)).asBytes())
        }
        cache.getAndPin(1001).let {
            it.putRecord(OidNameRecord(intField(4), stringField("table4"), booleanField(false)).asBytes())
            it.putRecord(OidNameRecord(intField(5), stringField("table5"), booleanField(false)).asBytes())
        }
        assertEquals(
            listOf("table2", "table3", "table4", "table5"),
            FullScanImpl(cache, NAME_SYSTABLE_OID, rootRecords::iterator).records {
                OidNameRecord(intField(), stringField(), booleanField()).fromBytes(it)
            }.map { it.value2 }.toList()
        )
    }

    @Test
    fun `full scan iterator will not pin pages`() {
        val storage = createHardDriveEmulatorStorage()
        val cache = SimplePageCacheImpl(storage, 3)
        cache.getAndPin(0).use {buf ->
            // We create 4 pages in the NAME system table
            buf.putRecord(OidPageidRecord(intField(NAME_SYSTABLE_OID), intField(1000)).asBytes())
            buf.putRecord(OidPageidRecord(intField(NAME_SYSTABLE_OID), intField(1001)).asBytes())
            buf.putRecord(OidPageidRecord(intField(NAME_SYSTABLE_OID), intField(1002)).asBytes())
            buf.putRecord(OidPageidRecord(intField(NAME_SYSTABLE_OID), intField(1003)).asBytes())
        }
        (1000..1003).forEach { pageId ->
            cache.getAndPin(pageId).use {
                val tableOid = pageId % 1000 + 1
                it.putRecord(OidNameRecord(intField(tableOid), stringField("table$tableOid"), booleanField(false)).asBytes())
            }
        }

        // We have a cache of size 3, so we expect that while iterating, we will not pin any pages and will not
        // get IllegalStateException
        val rootRecords = RootRecords(cache, 0, 1)
        assertEquals(
            listOf("table1", "table2", "table3", "table4"),
            FullScanImpl(cache, NAME_SYSTABLE_OID, rootRecords::iterator).records {
                OidNameRecord(intField(), stringField(), booleanField()).fromBytes(it)
            }.map { it.value2 }.toList()
        )
    }
}