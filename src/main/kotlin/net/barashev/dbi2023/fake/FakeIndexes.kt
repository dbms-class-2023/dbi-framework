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

package net.barashev.dbi2023.fake

import net.barashev.dbi2023.*
import java.util.function.Function
import kotlin.random.Random

class FakeIndex<S: AttributeType<T>, T: Comparable<T>>(
  private val storageAccessManager: StorageAccessManager,
  private val pageCache: PageCache,
  private val tableName: String,
  private val indexTableName: String,
  private val keyType: S,
  private val indexKey: Function<ByteArray, T>
): Index<T> {

    private val key2page = mutableMapOf<T, PageId>()

    internal fun buildIndex() {
        createIndexBuilder().use {tableBuilder ->
            storageAccessManager.createFullScan(tableName).pages().forEach { page ->
                page.allRecords().forEach { _, result ->
                    if (result.isOk) {
                        val indexKey = indexKey.apply(result.bytes)
                        val indexRecord = Record2(keyType to indexKey, intField(page.id))
                        tableBuilder.insert(indexRecord.asBytes())
                        key2page[indexKey] = page.id
                    }
                }
            }
        }
    }

    internal fun createIndexBuilder() = TableBuilder(storageAccessManager, pageCache, storageAccessManager.createTable(indexTableName))

    internal fun openIndex() {
        val parser = { bytes: ByteArray -> Record2(keyType to keyType.defaultValue(), intField()).fromBytes(bytes)}
        storageAccessManager.createFullScan(indexTableName).records(parser).forEach {
          key2page[it.value1] = it.value2
        }
    }

    override fun lookup(indexKey: T) = key2page[indexKey]?.let { listOf(it)} ?: emptyList()
}

class FakeMultiIndex<S: AttributeType<T>, T: Comparable<T>>(
    private val overflowTableName: String,
    private val storageAccessManager: StorageAccessManager,
    private val pageCache: PageCache,
    private val tableName: String,
    private val indexTableName: String,
    private val keyType: S,
    private val indexKey: Function<ByteArray, T>
): Index<T> {
    private val key2page = mutableMapOf<T, MutableList<PageId>>()
    fun buildIndex() {
        class MultiIndexEntryBuilder(
            val indexTableBuilder: TableBuilder,
            val overflowTableBuilder: TableBuilder,
            val indexKey: T,
            page: PageId,
            val overflowRunId: Int) {

            val pages = mutableListOf<PageId>()
            init {
                addPage(page)
            }

            fun addPage(page: PageId) = pages.add(page)

            fun insert() {
                if (pages.size == 1) {
                    indexTableBuilder.insert(Record3(keyType to indexKey, intField(pages[0]), intField(0)).asBytes())
                } else {
                    overflowTableBuilder.insert(Record2(intField(overflowRunId), intField(pages.size)).asBytes())
                    val overflowPageId = overflowTableBuilder.currentPage!!.id
                    indexTableBuilder.insert(Record3(keyType to indexKey, intField(-overflowPageId), intField(overflowRunId)).asBytes())
                    pages.forEach {
                        overflowTableBuilder.insert(Record2(intField(-1), intField(it)).asBytes())
                    }
                }
            }
        }

        // Sort the indexed values and associate them with the original page ids
        val auxTableName = "${indexTableName}.aux${Random.nextLong()}"
        val auxTableParser = { bytes: ByteArray -> Record2(keyType to keyType.defaultValue(), intField()).fromBytes(bytes) }
        val sortedByIndexKey = TableBuilder(storageAccessManager, pageCache, storageAccessManager.createTable(auxTableName)).use {auxBuilder ->
            storageAccessManager.createFullScan(tableName).pages().forEach { page ->
                page.allRecords().filter { it.value.isOk }.map { indexKey.apply(it.value.bytes) to page.id }.forEach {
                    auxBuilder.insert(Record2(keyType to it.first, intField(page.id)).asBytes())
                }
            }
            Operations.sortFactory(storageAccessManager, pageCache).sort(auxTableName) {
                auxTableParser(it).value1
            }
        }

        TableBuilder(storageAccessManager, pageCache, storageAccessManager.createTable(overflowTableName)).use {overflowTableBuilder ->
            createIndexBuilder().use {indexTableBuilder ->
                var overflowRunId = 1
                var entryBuilder: MultiIndexEntryBuilder? = null

                storageAccessManager.createFullScan(sortedByIndexKey).pages().forEach { page ->
                    page.allRecords().forEach { _, result ->
                        if (result.isOk) {
                            val indexKey = auxTableParser(result.bytes).value1
                            val dataPageId = auxTableParser(result.bytes).value2
                            entryBuilder = entryBuilder?.let {
                                if (it.indexKey == indexKey) {
                                    it.addPage(dataPageId)
                                    it
                                } else {
                                    it.insert()
                                    MultiIndexEntryBuilder(indexTableBuilder, overflowTableBuilder, indexKey, dataPageId, overflowRunId++)
                                }
                            } ?: run {
                                MultiIndexEntryBuilder(indexTableBuilder, overflowTableBuilder, indexKey, dataPageId, overflowRunId++)
                            }
                        }
                    }
                }
                entryBuilder?.insert()
            }
        }
        openIndex()

    }

    fun openIndex() {
        val parser = { bytes: ByteArray -> Record3(keyType to keyType.defaultValue(), intField(), intField()).fromBytes(bytes)}
        storageAccessManager.createFullScan(indexTableName).records(parser).forEach {indexRecord ->
            if (indexRecord.value3 == 0) {
                key2page.getOrPut(indexRecord.value1) { mutableListOf() }.add(indexRecord.value2)
            } else {
                storageAccessManager.createFullScan(overflowTableName).startAt(-indexRecord.value2).pages().takeWhile { page ->
                    page.allRecords().values.filter { it.isOk }.map {
                        Record2(intField(), intField()).fromBytes(it.bytes)
                    }.toList().let {overflowRecords ->
                        if (overflowRecords[0].value1 == -1 && key2page[indexRecord.value1] != null) {
                            val runRecords = overflowRecords.takeWhile { it.value1 == -1 }.toList()
                            runRecords.forEach {
                                key2page[indexRecord.value1]!!.add(it.value2)
                            }
                            runRecords.size == overflowRecords.size
                        } else {
                            var idxOverflow = overflowRecords.indexOfFirst { it.value1 == indexRecord.value3 }
                            while (idxOverflow != -1 && idxOverflow < overflowRecords.size && (overflowRecords[idxOverflow].value1 == indexRecord.value3 || overflowRecords[idxOverflow].value1 == -1)) {
                                if (overflowRecords[idxOverflow].value1 == -1) {
                                    val list = key2page.getOrPut(indexRecord.value1) { mutableListOf() }
                                    list.add(overflowRecords[idxOverflow].value2)
                                }
                                idxOverflow++;
                            }
                            idxOverflow == overflowRecords.size
                        }
                    }
                }
            }
        }
    }

    internal fun createIndexBuilder() = TableBuilder(storageAccessManager, pageCache, storageAccessManager.createTable(indexTableName))

    override fun lookup(indexKey: T) = key2page[indexKey] ?: emptyList()
}
class FakeIndexManager(private val storageAccessManager: StorageAccessManager, private val pageCache: PageCache): IndexManager {
    override fun <T : Comparable<T>, S : AttributeType<T>> build(
        tableName: String,
        indexTableName: String,
        method: IndexMethod,
        isUnique: Boolean,
        keyType: S,
        indexKey: Function<ByteArray, T>
    ): Index<T> =
        if (isUnique) {
            FakeIndex(storageAccessManager, pageCache, tableName, indexTableName, keyType, indexKey).also {
                it.buildIndex()
            }
        } else {
            FakeMultiIndex("${indexTableName}_overflow", storageAccessManager, pageCache, tableName, indexTableName, keyType, indexKey).also {
                it.buildIndex()
            }
        }

    override fun <T : Comparable<T>, S : AttributeType<T>> open(
        tableName: String,
        indexTableName: String,
        method: IndexMethod,
        isUnique: Boolean,
        keyType: S,
        indexKey: Function<ByteArray, T>
    ): Index<T> =
        if (isUnique) {
            FakeIndex(storageAccessManager, pageCache, tableName, indexTableName, keyType, indexKey).also {
                it.openIndex()
            }
        } else {
            FakeMultiIndex("${indexTableName}_overflow", storageAccessManager, pageCache, tableName, indexTableName, keyType, indexKey).also {
                it.openIndex()
            }
        }


}