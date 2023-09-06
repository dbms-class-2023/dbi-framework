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
import kotlin.random.Random
import kotlin.test.assertEquals

class StorageImplTest {
    @Test
    fun `create and read page`() {
        createHardDriveEmulatorStorage().let { storage ->
            val pageId1 = storage.read(1).let { page ->
                page.putRecord(TestRecord(1, 1).toByteArray())
                storage.write(page)
                page.id
            }
            val pageId2 = storage.read(2).let { page ->
                page.putRecord(TestRecord(2, 2).toByteArray())
                storage.write(page)
                page.id
            }
            storage.read(pageId1).let { page ->
                assertEquals(TestRecord(1,1), TestRecord.fromByteArray(page.getRecord(0).bytes))
            }
            storage.read(pageId2).let { page ->
                assertEquals(TestRecord(2,2), TestRecord.fromByteArray(page.getRecord(0).bytes))
            }
        }
    }

    @Test
    fun `create write and read page sequence`() {
        createHardDriveEmulatorStorage().let { storage ->
            val pageList = (1..10).map { idx ->
                storage.read(idx).also { page ->
                    page.putRecord(TestRecord(idx, idx).toByteArray())
                }
            }.toList()
            val writer = storage.bulkWrite(11)
            val writtenPages = pageList.map { writer.write(it) }.toList()
            writer.close()

            var idx = 1
            storage.bulkRead(writtenPages.first().id, writtenPages.size) { page ->
                assertEquals(TestRecord(idx, idx), TestRecord.fromByteArray(page.getRecord(0).bytes))
                idx++
            }
        }
    }

    @Test
    fun `read and write random page`() {
        createHardDriveEmulatorStorage().let { storage ->
            repeat(42) {
                storage.read(Random.nextInt(100)).let { page ->
                    page.putRecord(TestRecord(page.id, page.id).toByteArray())
                    storage.write(page)
                }
            }
            (0..100).forEach {
                storage.read(it).let { page ->
                    if (page.allRecords().isNotEmpty()) {
                        assertEquals(TestRecord(page.id, page.id), TestRecord.fromByteArray(page.getRecord(0).bytes))
                    }
                }
            }
        }
    }



}