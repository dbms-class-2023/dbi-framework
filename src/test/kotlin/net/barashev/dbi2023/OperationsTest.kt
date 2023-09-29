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
 */

package net.barashev.dbi2023

import net.barashev.dbi2023.fake.FakeHashTableBuilder
import net.barashev.dbi2023.fake.FakeMergeSort
import net.datafaker.Faker
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class OperationsTest  {
    @Test
    fun `merge sort smoke test`() {
        Operations.sortFactory = { accessMethodManager, pageCache -> FakeMergeSort(accessMethodManager, pageCache) }
        val storage = createHardDriveEmulatorStorage()
        val cache = FifoPageCacheImpl(storage, 20)
        val accessMethodManager = SimpleStorageAccessManager(cache)
        val fooOid = accessMethodManager.createTable("foo")
        TableBuilder(accessMethodManager, cache, fooOid).let {builder ->
            (1..10000).shuffled().forEach {
                builder.insert(intField().first.asBytes(it))
            }
        }
        val outTable = Operations.sortFactory(accessMethodManager, cache).sort("foo") {
            intField().first.fromBytes(it).first
        }
        assertEquals((1..10000).toList(), accessMethodManager.createFullScan(outTable).records {
            intField().first.fromBytes(it).first
        }.toList())
    }

    @Test
    fun `hash table smoke test`() {
        Operations.hashFactory = { storageAccessManager, pageCache -> FakeHashTableBuilder(storageAccessManager, pageCache) }

        val faker = Faker()
        val storage = createHardDriveEmulatorStorage()
        val cache = FifoPageCacheImpl(storage, 20)
        val accessMethodManager = SimpleStorageAccessManager(cache)
        val fooOid = accessMethodManager.createTable("foo")
        TableBuilder(accessMethodManager, cache, fooOid).let {builder ->
            (1..10000).shuffled().forEach {
                builder.insert(Record2(intField(it), stringField(faker.name().fullName())).asBytes())
            }
        }
        val hashTable = Operations.hashFactory(accessMethodManager, cache).let { builder ->
            builder.hash("foo", 10) {
                Record2(intField(), stringField()).fromBytes(it).value1
            }
        }
        assertEquals(10, hashTable.buckets.size)
        (1..10000).forEach {
            assertTrue(hashTable.find(it).toList().isNotEmpty())
        }
        assertTrue(hashTable.find(10001).toList().isEmpty())

    }
}