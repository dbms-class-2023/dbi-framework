/*
 * Copyright 2023 Dmitry Barashev, JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.barashev.dbi2023

import net.barashev.dbi2023.app.initializeFactories
import net.barashev.dbi2023.catalog.CatalogPageFactoryImpl
import net.datafaker.Faker
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class IndexesTest {
    val faker = Faker()
    lateinit var storage: Storage
    lateinit var directoryStorage: Storage
    @BeforeEach
    fun initialize() {
        storage = createHardDriveEmulatorStorage()
        directoryStorage = createHardDriveEmulatorStorage(CatalogPageFactoryImpl())
        initializeFactories(storage)
    }

    @Test
    fun `smoke test`() {
        val indexMethod = IndexMethod.valueOf(System.getProperty("index.method", "BTREE"))
        val cache = SimplePageCacheImpl(storage, 20)
        val accessMethodManager = SimpleStorageAccessManager(cache, directoryStorage)


        val allNames = mutableListOf<String>()
        val fooOid = accessMethodManager.createTable("foo")
        cache.getAndPin(accessMethodManager.addPage(fooOid)).use {inPage ->
            (1..100).forEach { inPage.putRecord(Record2(intField(it), stringField(faker.name().fullName().also {allNames.add(it)})).asBytes()) }
        }

        val indexManager = Indexes.indexFactory(accessMethodManager, cache)
        val index1 = indexManager.build("foo", "foo_id_idx", method = indexMethod, isUnique = true, keyType = IntAttribute()) {
            Record2(intField(), stringField()).fromBytes(it).value1
        }
        assertFalse(index1.lookup(10).isEmpty())
        val index1a = indexManager.open("foo", "foo_id_idx", method = indexMethod, isUnique = true, keyType = IntAttribute()) {
            Record2(intField(), stringField()).fromBytes(it).value1
        }
        assertEquals(index1.lookup(10), index1a.lookup(10))


        val index2 = indexManager.build("foo", "foo_name_idx", method = indexMethod, isUnique = true, keyType = StringAttribute()) {
            Record2(intField(), stringField()).fromBytes(it).value2
        }
        val index2a = indexManager.open("foo", "foo_name_idx", method = indexMethod, isUnique = true, keyType = StringAttribute()) {
            Record2(intField(), stringField()).fromBytes(it).value2
        }
        assertFalse(index2.lookup(allNames[0]).isEmpty())
        assertEquals(index2.lookup(allNames[0]), index2a.lookup(allNames[0]))
    }

    @Test
    fun `multi index test`() {
        val indexMethod = IndexMethod.valueOf(System.getProperty("index.method", "BTREE"))
        val cache = SimplePageCacheImpl(storage, 20)
        val accessMethodManager = SimpleStorageAccessManager(cache, directoryStorage)

        var fizzCount = 0
        var buzzCount = 0
        var fizzbuzzCount = 0
        TableBuilder(accessMethodManager, cache, accessMethodManager.createTable("FizzBuzz")).use {fizzBuzzTablebuilder ->
            (1..10000).forEach {
                if (it % 3 == 0 && it % 5 == 0) {
                    fizzBuzzTablebuilder.insert(Record2(intField(it), stringField("fizzbuzz")).asBytes())
                    fizzbuzzCount++
                } else if (it % 3 == 0) {
                    fizzBuzzTablebuilder.insert(Record2(intField(it), stringField("fizz")).asBytes())
                    fizzCount++
                } else if (it % 5 == 0) {
                    fizzBuzzTablebuilder.insert(Record2(intField(it), stringField("buzz")).asBytes())
                    buzzCount++
                } else {
                    fizzBuzzTablebuilder.insert(Record2(intField(it), stringField("$it")).asBytes())
                }
            }
        }

        val indexManager = Indexes.indexFactory(accessMethodManager, cache)
        val index1 = indexManager.build("FizzBuzz", "fizzbuzz_value_idx", method = indexMethod, isUnique = false, keyType = StringAttribute()) {
            Record2(intField(), stringField()).fromBytes(it).value2
        }
        assertEquals(fizzCount, index1.lookup("fizz").size)
        assertEquals(buzzCount, index1.lookup("buzz").size)
        assertEquals(fizzbuzzCount, index1.lookup("fizzbuzz").size)
        assertTrue(index1.lookup("aas").isEmpty())
        assertEquals(1, index1.lookup("1").size)
    }

    @Test
    fun `overflow page, an overflow run start is the last record`() {
        val cache = SimplePageCacheImpl(storage, 20)
        val accessMethodManager = SimpleStorageAccessManager(cache, directoryStorage)

        val overflowTableOid = accessMethodManager.createTable("overflow_table")
        val overflowPage1 = accessMethodManager.addPage(overflowTableOid).let(cache::getAndPin).use {
            // The first overflow page contains a single head of overflow run #1
            it.putRecord(Record2(intField(1), intField(3)).asBytes())
            it.id
        }
        accessMethodManager.addPage(overflowTableOid).let(cache::getAndPin).use {
            // The second page  contains three overflow run records.
            it.putRecord(Record2(intField(-1), intField(1)).asBytes())
            it.putRecord(Record2(intField(-1), intField(2)).asBytes())
            it.putRecord(Record2(intField(-1), intField(3)).asBytes())
        }

        val overflowReader = IndexOverflowReader(accessMethodManager, "overflow_table")
        assertEquals(listOf(1, 2, 3), overflowReader.lookup(overflowPage1, 1))
    }
}