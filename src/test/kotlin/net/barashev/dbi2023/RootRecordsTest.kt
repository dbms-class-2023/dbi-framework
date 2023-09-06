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

class RootRecordsTest {
    @Test
    fun `empty root records`() {
        val storage = createHardDriveEmulatorStorage()
        val cache = SimplePageCacheImpl(storage, 20)
        assertEquals(listOf(), RootRecords(cache, 0, 1).toList())
    }
    @Test
    fun `iterate in bounds of one page`() {
        val storage = createHardDriveEmulatorStorage()
        val cache = SimplePageCacheImpl(storage, 20)
        cache.getAndPin(0).use {buf ->
            buf.putRecord(OidPageidRecord(intField(1), intField(1000)).asBytes())
            buf.putRecord(OidPageidRecord(intField(1), intField(1001)).asBytes())
            buf.putRecord(OidPageidRecord(intField(2), intField(2000)).asBytes())
            buf.putRecord(OidPageidRecord(intField(1), intField(1002)).asBytes())
            buf.putRecord(OidPageidRecord(intField(2), intField(2001)).asBytes())
            buf.putRecord(OidPageidRecord(intField(3), intField(3000)).asBytes())
        }


        assertEquals(listOf(1000, 1001, 2000, 1002, 2001, 3000), RootRecords(cache, 0, 1).map { it.value2 }.toList())
    }

    @Test
    fun `iterate over several pages`() {
        val storage = createHardDriveEmulatorStorage()
        val cache = SimplePageCacheImpl(storage, 3)
        cache.getAndPin(0).use {buf ->
            buf.putRecord(OidPageidRecord(intField(1), intField(1000)).asBytes())
            buf.putRecord(OidPageidRecord(intField(1), intField(1001)).asBytes())
        }
        cache.getAndPin(1).use {buf ->
            buf.putRecord(OidPageidRecord(intField(2), intField(2000)).asBytes())
            buf.putRecord(OidPageidRecord(intField(1), intField(1002)).asBytes())
        }
        cache.getAndPin(2).use { buf ->
            buf.putRecord(OidPageidRecord(intField(2), intField(2001)).asBytes())
            buf.putRecord(OidPageidRecord(intField(3), intField(3000)).asBytes())
        }
        cache.getAndPin(3).use { buf ->
            buf.putRecord(OidPageidRecord(intField(4), intField(4000)).asBytes())
            buf.putRecord(OidPageidRecord(intField(4), intField(4001)).asBytes())
        }

        assertEquals(listOf(1000, 1001, 2000, 1002, 2001, 3000, 4000, 4001), RootRecords(cache, 0, 4)
            .map { it.value2 }.toList())

    }

}
