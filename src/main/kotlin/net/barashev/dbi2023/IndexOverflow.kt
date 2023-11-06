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

import java.util.function.Function
import kotlin.random.Random

/**
 * Interface for communication between the overflow builder and the main index structure. Index structure implementation
 * may decide on its own the way it stores single keys and multi-keys.
 */
interface IndexBuilder<T: Comparable<T>> {
    /**
     * Inserts a single indexKey into the index structure, referencing a dataPage.
     */
    fun insertSingleKey(indexKey: T, dataPage: PageId)

    /**
     * Inserts an indexKey into the index structure, referencing an overflow run with id=overflowRunId, located
     * at the overflowPage.
     */
    fun insertMultiKey(indexKey: T, overflowPage: PageId, overflowRunId: Int)
}

/**
 * This class scans the table that is being indexed and builds an index structure, using the provided IndexBuilder,
 * and an overflow storage.
 */
class IndexOverflowBuilder<T: Comparable<T>>(
    private val accessManager: StorageAccessManager,
    private val pageCache: PageCache,
    private val dataTableName: String,
    private val overflowTableName: String,
    private val indexKeyType: AttributeType<T>,
    private val indexKeyParser: Function<ByteArray, T>,
    private val indexBuilder: IndexBuilder<T>) {

    private var sortedByIndexKey: String = ""
    private val auxTableName = "$dataTableName.aux${Random.nextLong()}"

    fun build() = try {
        doBuild()
    } finally {
        accessManager.deleteTable(auxTableName)
        accessManager.deleteTable(sortedByIndexKey)
    }
    private fun doBuild() {
        // Scan the entire input table, sort the index keys and associate each key with the page id where the original
        // data record is located.
        // We write the sorted pairs of (indexKey, dataPageId) into the auxiliary table, and then sort it.
        val auxTableParser = { bytes: ByteArray ->
            Record2(indexKeyType to indexKeyType.defaultValue(), intField()).fromBytes(bytes)
        }
        sortedByIndexKey = TableBuilder(accessManager, pageCache, accessManager.createTable(auxTableName)).use {auxBuilder ->
            accessManager.createFullScan(dataTableName).pages().forEach { page ->
                page.allRecords().filter { it.value.isOk }.map { indexKeyParser.apply(it.value.bytes) to page.id }.forEach {
                    auxBuilder.insert(Record2(indexKeyType to it.first, intField(page.id)).asBytes())
                }
            }
            Operations.sortFactory(accessManager, pageCache).sort(auxTableName) {
                auxTableParser(it).value1
            }
        }

        // Now scan the sorted index keys and for each key
        // - insert it into the index if there are no duplicates
        // - create an overflow run on an overflow page and insert a pair (overflow page id, overflow run id)
        //   into the index.
        TableBuilder(accessManager, pageCache, accessManager.createTable(overflowTableName)).use {overflowTableBuilder ->
            var overflowRunId = 1
            var entryBuilder: MultiIndexEntryBuilder<T>? = null

            accessManager.createFullScan(sortedByIndexKey).records(auxTableParser).forEach { auxRecord ->
                val indexKey = auxRecord.value1
                val dataPageId = auxRecord.value2
                entryBuilder = entryBuilder?.let {
                    if (it.indexKey == indexKey) {
                        it.addPage(dataPageId)
                        it
                    } else {
                        it.insert()
                        MultiIndexEntryBuilder(indexBuilder, overflowTableBuilder, indexKey, dataPageId, overflowRunId++)
                    }
                } ?: run {
                    MultiIndexEntryBuilder(indexBuilder, overflowTableBuilder, indexKey, dataPageId, overflowRunId++)
                }
            }
            entryBuilder?.insert()
        }
    }
}

/**
 * This class reads the page identifiers from the overflow pages by given overflow page id and overflow run id.
 */
class IndexOverflowReader(
    private val accessManager: StorageAccessManager,
    private val overflowTableName: String) {

    /**
     * Reads the data page ids from the overflow run with id=overflowRunId located at the overflowPage.
     * @return a list of data page identifiers. If an overflow run is not found, returns an empty list.
     */
    fun lookup(overflowPage: PageId, overflowRunId: Int): List<PageId> {
        val result = mutableListOf<PageId>()
        accessManager.createFullScan(overflowTableName).startAt(overflowPage).pages().takeWhile { page ->
            val overflowRecords = page.allRecords().values.filter { it.isOk }.map {
                Record2(intField(), intField()).fromBytes(it.bytes)
            }.toList()
            if (overflowRecords[0].value1 == -1 && result.isNotEmpty()) {
                // This is the case when the overflow run spans a few pages.
                val runRecords = overflowRecords.takeWhile { it.value1 == -1 }.map { it.value2 }.toList()
                result.addAll(runRecords)
                // If the first component of all overflow records is -1 then our overflow run continues at the next page.
                runRecords.size == overflowRecords.size
            } else {
                // Otherwise it is the first page where the overflow run starts. We need to find
                // the record from which it actually starts.
                var idxOverflow = overflowRecords.indexOfFirst { it.value1 == overflowRunId }
                while (
                    idxOverflow != -1 && // we have found the overflow run start
                    idxOverflow < overflowRecords.size && // and we are not yet out of the record list
                    (overflowRecords[idxOverflow].value1 == overflowRunId || overflowRecords[idxOverflow].value1 == -1)
                    // We are one of the run records. If we have many runs per page, we will stop as soon as we reach
                    // the next run start.
                ) {
                    if (overflowRecords[idxOverflow].value1 == -1) {
                        result.add(overflowRecords[idxOverflow].value2)
                    } else {
                        // We are at the overflow run start. Should it be the last records on the page, we would not add anything
                        // to the result. So we add a fake page id = -1, and we'll remove it later.
                        result.add(-1)
                    }
                    idxOverflow++;
                }
                // We continue if we reach the last record.
                idxOverflow == overflowRecords.size
            }
        }
        // The first element of the result is fake -1.
        return result.also { it.removeFirst() }
    }
}

private class MultiIndexEntryBuilder<T: Comparable<T>>(
    private val indexBuilder: IndexBuilder<T>,
    private val overflowTableBuilder: TableBuilder,
    val indexKey: T,
    page: PageId,
    private val overflowRunId: Int) {

    private val pages = mutableListOf<PageId>()
    init {
        addPage(page)
    }

    fun addPage(page: PageId) = pages.add(page)

    fun insert() {
        if (pages.size == 1) {
            indexBuilder.insertSingleKey(indexKey, pages[0])
        } else {
            // The overflow run starts with a record (run ID, run length) ...
            overflowTableBuilder.insert(Record2(intField(overflowRunId), intField(pages.size)).asBytes())
            val overflowPageId = overflowTableBuilder.currentPage!!.id
            indexBuilder.insertMultiKey(indexKey, overflowPageId, overflowRunId)
            // ... and continues with a sequence of records (-1, data page ID)
            pages.forEach {
                overflowTableBuilder.insert(Record2(intField(-1), intField(it)).asBytes())
            }
        }
    }
}
