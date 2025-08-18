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

import net.barashev.dbi2023.catalog.CatalogPageFactoryImpl
import org.junit.jupiter.api.Test
import kotlin.random.Random
import kotlin.test.*

class DiskPageImplTest {
    @Test
    fun `check empty page properties`() {
        createDiskPage().let {
            assertTrue(it.getRecord(0).isOutOfRange)
        }
    }

    @Test
    fun `add and get record`() {
        createDiskPage().let {
            val record = TestRecord(42, 1)
            assertTrue(it.putRecord(record.toByteArray()).isOk)

            assertEquals(record, TestRecord.fromByteArray(it.getRecord(0).bytes))

        }
    }

    @Test
    fun `add and update record`() {
        createDiskPage().let {
            assertTrue(it.putRecord(TestRecord(42, 1).toByteArray()).isOk)
            val newRecord = TestRecord(43, 2)
            assertTrue(it.putRecord(newRecord.toByteArray(), 0).isOk)

            assertEquals(newRecord, TestRecord.fromByteArray(it.getRecord(0).bytes))
        }
    }

    @Test
    fun `add many records`() {
        createDiskPage().let { page ->
            for (i in   0..100) {
                val rec = TestRecord(i, i)
                assertTrue(page.putRecord(rec.toByteArray(), i).isOk)
            }
            for (i in 0..100) {
                val rec = TestRecord(i, i)
                assertEquals(rec, TestRecord.fromByteArray(page.getRecord(i).bytes))
            }
        }
    }

    @Test
    fun `putRecord failures`() {
        createDiskPage().let { page ->
            val bytes = ByteArray(1300)
            assertTrue(page.putRecord(bytes, 0).isOk)
            assertTrue(page.putRecord(bytes, 1).isOk)
            assertTrue(page.putRecord(bytes, 2).isOk)
            assertFalse(page.putRecord(bytes, 3).isOk)
            assertTrue(page.putRecord(bytes, 3).isOutOfSpace)

            assertFalse(page.putRecord(bytes, 5).isOk)
            assertTrue(page.putRecord(bytes, 5).isOutOfRange)
        }
    }

    @Test
    fun `grow records`() {
        createDiskPage().let { page ->
            for (i in 0..10) {
                val rec = TestRecord(i, i)
                assertTrue(page.putRecord(rec.toByteArray(), i).isOk)
            }
            for (i in 0..10) {
                val rec = VarLengthRecord().also {
                    it.key = i
                    it.value = Random.nextLong(100000).toString()
                }
                assertTrue(page.putRecord(rec.toByteArray(), i).isOk)
                assertEquals(rec, VarLengthRecord.fromByteArray(page.getRecord(i).bytes))
                for (j in i+1..10) {
                    assertEquals(TestRecord(j ,j), TestRecord.fromByteArray(page.getRecord(j).bytes))
                }
            }
        }
    }

    @Test
    fun `shrink records`() {
        createDiskPage().let { page ->
            for (i in 0..10) {
                val rec = VarLengthRecord().also {
                    it.key = i
                    it.value = Random.nextLong(100000).toString()
                }
                assertTrue(page.putRecord(rec.toByteArray(), i).isOk)
            }
            for (i in 0..10) {
                val rec = TestRecord(i, i)
                assertTrue(page.putRecord(rec.toByteArray(), i).isOk)
                assertEquals(rec, TestRecord.fromByteArray(page.getRecord(i).bytes))
                for (j in i+1..10) {
                    assertEquals(j, VarLengthRecord.fromByteArray(page.getRecord(j).bytes).key)
                    assertTrue(VarLengthRecord.fromByteArray(page.getRecord(j).bytes).value.isNotBlank())
                }

            }
        }
    }

    @Test
    fun `preserve deleted records when resizing`() {
        createDiskPage().let { page ->
            for (i in 0..3) {
                assertTrue(page.putRecord(TestRecord(i, i).toByteArray(), i).isOk)
            }
            page.deleteRecord(2)
            assertTrue(page.getRecord(2).isDeleted)
            assertFalse(page.getRecord(2).isOk)
            assertTrue(page.putRecord(VarLengthRecord().also {
                it.key = 0
                it.value = "Foo bar"
            }.toByteArray(), 0).isOk)
            assertTrue(page.getRecord(2).isDeleted)
        }
    }

    @Test
    fun `all records`() {
        createDiskPage().let { page ->
            for (i in 0..3) {
                assertTrue(page.putRecord(TestRecord(i, i).toByteArray(), i).isOk)
            }
            page.deleteRecord(2)
            page.allRecords().let {
                assertEquals(TestRecord(0, 0), TestRecord.fromByteArray(it[0]!!.bytes))
                assertEquals(TestRecord(1, 1), TestRecord.fromByteArray(it[1]!!.bytes))
                assertEquals(TestRecord(3, 3), TestRecord.fromByteArray(it[3]!!.bytes))
                assertNotNull(it[2])
                assertTrue(it[2]!!.isDeleted)

            }
        }
    }

    @Test
    fun `delete and put first record`() {
        val storage = createHardDriveEmulatorStorage()
        val catalogStorage = createHardDriveEmulatorStorage(CatalogPageFactoryImpl())
        val cache = SimplePageCacheImpl(storage, 1)
        val accessManager = SimpleStorageAccessManager(cache, catalogStorage)

        val tableName = "exampleTable"

        val tableOid = accessManager.createTable(tableName)
        val pageId1 = accessManager.addPage(tableOid)

        cache.getAndPin(pageId1).use {
            it.putRecord(Record1(intField(1)).asBytes())
            it.deleteRecord(0)
        }

        // evict page 1, needed to re-initialize disk page fielsd again
        cache.getAndPin(accessManager.addPage(tableOid)).use {}

        cache.getAndPin(pageId1).use {
            val res = it.putRecord(Record1(intField(2)).asBytes(), 0)
            assertTrue(res.isOk)
        }
    }
}