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

/*
 * This file provides implementations of iterators over disk page records.
 */
package net.barashev.dbi2023

import java.util.function.Function

/**
 * Base class for the root records iterator and regular table iterator.
 */
internal abstract class FullScanIteratorBase<T>(
    private val recordBytesParser: Function<ByteArray, T>
): Iterator<T> {

    private var currentRecordIdx = 0
    private var currentRecord: T? = null
    private var currentPageRecords: Map<RecordId, T>? = null

    override fun hasNext() = currentRecord != null

    override fun next(): T = currentRecord!!.also { advance() }

    protected fun advance() {
        val nextRecord = currentPageRecords?.get(currentRecordIdx)
        if (nextRecord == null) {
            val nextPage = advancePage()
            if (nextPage == null) {
                currentRecord = null
                return
            }
            currentPageRecords =
                nextPage.allRecords().let {records ->
                    records.filterValues { it.isOk }.mapValues { recordBytesParser.apply(it.value.bytes) }
                }
            currentRecordIdx = 0
            advance()
            return
        } else {
            currentRecordIdx += 1
            currentRecord = nextRecord
            return
        }
    }

    protected abstract fun advancePage(): CachedPage?
}

/**
 * This is a root records iterator which scans over the root records which map table oid to table pages.
 * By default it starts from page #1 and sequentially iterates until MAX_ROOT_PAGE_COUNT are read.
 * However, the pages to read can be changed with the constructor arguments.
 */
internal class RootRecordIteratorImpl(
    private val pageCache: PageCache,
    private val startRootPageId: PageId = 1,
    private val maxRootPageCount: Int = MAX_ROOT_PAGE_COUNT): FullScanIteratorBase<OidPageidRecord>({
    OidPageidRecord(intField(), intField()).fromBytes(it)
}) {

    private var currentRootPageId = -1
    init {
        currentRootPageId = startRootPageId - 1
        advance()
    }

    override fun advancePage(): CachedPage? {
        currentRootPageId += 1
        return if (currentRootPageId >= startRootPageId + maxRootPageCount) {
            null
        } else {
            pageCache.get(currentRootPageId)
        }
    }
}

/**
 * This class iterates over the regular table pages. It takes ids of the pages to read from
 * the supplied root records iterator, by filtering records with the specified table OID.
 */
internal class FullScanIteratorImpl<T>(
    pageCache: PageCache,
    tableOid: Oid,
    rootRecords: Iterator<OidPageidRecord>,
    recordBytesParser: Function<ByteArray, T>): FullScanIteratorBase<T>(recordBytesParser) {

    private val pageIterator = PageIteratorImpl(pageCache, tableOid, rootRecords)

    init {
        advance()
    }
    override fun advancePage(): CachedPage? =
        if (pageIterator.hasNext()) {
            pageIterator.next()
        } else null

}

internal class PageIteratorImpl(private val pageCache: PageCache,
                                private val tableOid: Oid,
                                private val rootRecords: Iterator<OidPageidRecord>): Iterator<CachedPage> {

    private var currentPage: CachedPage? = null
    init {
        currentPage = advancePage()
    }
    override fun hasNext(): Boolean = currentPage != null

    override fun next(): CachedPage {
        return currentPage!!.also { currentPage = advancePage() }
    }

    private fun advancePage(): CachedPage? {
        while (rootRecords.hasNext()) {
            val nextOidPageidRecord = rootRecords.next()
            if (nextOidPageidRecord.value1 == tableOid) {
                return pageCache.get(nextOidPageidRecord.value2)
            }
        }
        return null
    }
}

/**
 * Full scan implementation.
 */
class FullScanImpl(
    private val pageCache: PageCache,
    private val tableOid: Oid,
    private val rootRecords: () -> Iterator<OidPageidRecord>) : FullScan {
    override fun pages(): Iterable<CachedPage> {
        return Iterable { PageIteratorImpl(pageCache, tableOid, rootRecords()) }
    }

    override fun <T> records(recordBytesParser: Function<ByteArray, T>): Iterable<T> {
        return Iterable {
            FullScanIteratorImpl(pageCache, tableOid, rootRecords(), recordBytesParser)
        }
    }
}

/**
 * Iterable wrapper for creating root records iterator instances.
 */
class RootRecords(private val pageCache: PageCache,
                  private val startRootPageId: PageId = 1,
                  private val maxRootPageCount: Int = MAX_ROOT_PAGE_COUNT): Iterable<OidPageidRecord> {
    override fun iterator(): Iterator<OidPageidRecord> = RootRecordIteratorImpl(pageCache, startRootPageId, maxRootPageCount)
}
