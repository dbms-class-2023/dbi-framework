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

/**
 * This fake index keeps the entire mapping of the index keys to page ids in memory, however,
 * it can persist the mapping to the disk and read it back from the disk.
 * This index assumes that all index keys are unique,
 */
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

/**
 * This fake index keeps the entire mapping of the index keys to page ids in memory, however,
 * it can persist the mapping to the disk and read it back from the disk.
 *
 * This index assumes that there may be index key duplicates. If there are many values of some index keys, the data page
 * references are stored in the overflow storage. However, if some key has no duplicates, the reference to the data page
 * is stored directly in the index structure.
 */
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
        TableBuilder(storageAccessManager, pageCache, storageAccessManager.createTable(indexTableName)).use {indexTableBuilder ->
            val indexBuilder = object : IndexBuilder<T> {
                private val SINGLE_KEY_ENTRY = intField(0)
                override fun insertSingleKey(indexKey: T, dataPage: PageId) {
                    indexTableBuilder.insert(Record3(keyType to indexKey, intField(dataPage), SINGLE_KEY_ENTRY).asBytes())
                }

                override fun insertMultiKey(indexKey: T, overflowPage: PageId, overflowRunId: Int) {
                    indexTableBuilder.insert(Record3(keyType to indexKey, intField(-overflowPage), intField(overflowRunId)).asBytes())
                }
            }
            IndexOverflowBuilder(storageAccessManager, pageCache, tableName, overflowTableName, keyType, indexKey, indexBuilder).let {
                it.build()
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
                IndexOverflowReader(storageAccessManager, overflowTableName).lookup(-indexRecord.value2, indexRecord.value3).let {
                    key2page[indexRecord.value1] = it.toMutableList()
                }
            }
        }
    }

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