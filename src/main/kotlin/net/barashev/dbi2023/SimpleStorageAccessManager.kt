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

import java.lang.IllegalArgumentException
import java.util.Collections
import java.util.function.Function

private const val ATTRIBUTE_SYSTABLE_OID = 1

const val NAME_SYSTABLE_OID = 0


internal interface TablePageDirectory {
    fun pages(tableOid: Oid): Iterable<OidPageidRecord>
    fun add(tableOid: Oid, pageCount: Int = 1): PageId
    fun delete(tableOid: Oid)
}

/**
 * This simple implementation of the page directory allocates one disk page per table, where ids of all
 * table pages are stored. Consequently, the total number of a table pages is restricted by the number of
 * directory records that fit into a single directory page. Also, the total number of pages is restricted by a constant
 * which indicates how many pages are allocated for the directory.
 */
class SimplePageDirectoryImpl(private val pageCache: PageCache): TablePageDirectory {
    private var maxPageId: PageId = MAX_ROOT_PAGE_COUNT + 1
    override fun pages(tableOid: Oid): Iterable<OidPageidRecord> = RootRecords(pageCache, tableOid, 1)

    override fun add(tableOid: Oid, pageCount: Int): PageId {
        val nextPageId = maxPageId
        maxPageId += pageCount
        return pageCache.getAndPin(tableOid).use { cachedPage ->
            (nextPageId until maxPageId).forEach {
                cachedPage.putRecord(OidPageidRecord(intField(tableOid), intField(it)).asBytes()).let {result ->
                    if (result.isOutOfSpace) {
                        throw AccessMethodException("Directory page overflow for relation $tableOid. The directory page already contains ${cachedPage.allRecords().size} records")
                    }
                }
            }
            nextPageId
        }
    }

    override fun delete(tableOid: Oid) {
        pageCache.getAndPin(tableOid).use {
            it.clear()
        }
    }
}

/**
 * In-memory cache of table name to table OID mapping, which
 * loads data from the system table NAME_SYSTABLE.
 */
internal class TableOidMapping(
    private val pageCache: PageCache,
    private val tablePageDirectory: TablePageDirectory) {
    private val cachedMapping = mutableMapOf<String, Oid?>()

    private fun createAccess(): Iterable<OidNameRecord> =
        FullScanImpl(pageCache, NAME_SYSTABLE_OID, {tablePageDirectory.pages(NAME_SYSTABLE_OID).iterator()}).records {
            OidNameRecord(intField(), stringField(), booleanField()).fromBytes(it)
        }

    fun get(tableName: String): Oid? {
        val oid = cachedMapping.getOrPut(tableName) {
            createAccess().firstOrNull {
                it.value2 == tableName && !it.value3
            }?.value1 ?: -1
        }
        return if (oid == -1) null else oid
    }

    internal fun isValid(oid: Oid): Boolean =
        createAccess().firstOrNull {
            it.value1 == oid && !it.value3
        } != null

    fun create(tableName: String): Oid {
        val nextOid = nextTableOid()
        val record = OidNameRecord(intField(nextOid), stringField(tableName), booleanField(false))
        val bytes = record.asBytes()

        val isOk = tablePageDirectory.pages(NAME_SYSTABLE_OID).firstOrNull { oidPageId ->
            pageCache.getAndPin(oidPageId.value2).use { nameTablePage ->
                nameTablePage.putRecord(bytes).isOk
            }
        } != null
        if (!isOk) {
            tablePageDirectory.add(NAME_SYSTABLE_OID).let {
                pageCache.getAndPin(it).use { nameTablePage ->
                    nameTablePage.putRecord(bytes).isOk
                }
            }
        }
        return nextOid.also {
            cachedMapping[tableName] = it
        }
    }

    private fun nextTableOid(): Oid {
        var maxOid = NAME_SYSTABLE_OID
        createAccess().forEach {
            // We ignore "isDeleted" flag here to be sure that table oid are always unique
            maxOid = maxOf(maxOid, it.value1)
        }
        return maxOid + 1
    }

    fun delete(tableName: String) {
        FullScanImpl(pageCache, NAME_SYSTABLE_OID, {tablePageDirectory.pages(NAME_SYSTABLE_OID).iterator()}).pages().forEach {page ->
            page.allRecords().entries.mapNotNull {
                if (it.value.isOk) {
                    it.key to OidNameRecord(intField(), stringField(), booleanField()).fromBytes(it.value.bytes)
                } else null
            }.filter {
                it.second.component2() == tableName
            }.forEach {
                page.putRecord(OidNameRecord(intField(it.second.value1), stringField(it.second.value2), booleanField(true)).asBytes(), it.first)
            }
        }
        cachedMapping.remove(tableName)
    }
}


class SimpleStorageAccessManager(private val pageCache: PageCache): StorageAccessManager {
    private val tablePageDirectory = SimplePageDirectoryImpl(pageCache)
    private val tableOidMapping = TableOidMapping(pageCache, tablePageDirectory)

    override fun createFullScan(tableName: String): FullScan =
        tableOidMapping.get(tableName)
            ?.let { tableOid ->
                FullScanImpl(pageCache, tableOid) { RootRecordIteratorImpl(pageCache, tableOid, 1) }
            }
            ?: throw AccessMethodException("Relation $tableName not found")

    override fun createTable(tableName: String, vararg columns: Triple<String, AttributeType<Any>, ColumnConstraint?>): Oid {
        if (tableOidMapping.get(tableName) != null) {
            throw IllegalArgumentException("Table $tableName already exists")
        }
        return tableOidMapping.create(tableName)
    }

    override fun addPage(tableOid: Oid, pageCount: Int): PageId =
        if (tableOidMapping.isValid(tableOid)) {
            tablePageDirectory.add(tableOid, pageCount)
        } else {
            throw AccessMethodException("Table with oid $tableOid not found")
        }

    override fun pageCount(tableName: String): Int =
        tableOidMapping.get(tableName)?.let {
            tablePageDirectory.pages(it).count()
        } ?: throw AccessMethodException("Relation $tableName not found")

    override fun tableExists(tableName: String): Boolean = tableOidMapping.get(tableName) != null

    override fun deleteTable(tableName: String) {
        // This implementation just removes table records from the table page directory and from the name=>oid
        // mapping. It won't clear or garbage-collect table pages.
        tableOidMapping.get(tableName)?.let {
            tablePageDirectory.delete(it)
            tableOidMapping.delete(tableName)
        }
    }
}