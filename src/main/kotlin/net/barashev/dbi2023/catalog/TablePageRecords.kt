package net.barashev.dbi2023.catalog

import net.barashev.dbi2023.CachedPage
import net.barashev.dbi2023.FullScanIteratorBase
import net.barashev.dbi2023.Oid
import net.barashev.dbi2023.OidPageidRecord
import net.barashev.dbi2023.PageCache
import net.barashev.dbi2023.PageId
import net.barashev.dbi2023.intField

class TablePageRecords(
    private val pageCache: PageCache,
    private val tableId: Oid
): Iterable<OidPageidRecord> {

    override fun iterator(): Iterator<OidPageidRecord> = TablePageRecordIteratorImpl(pageCache, tableId)
}

private val TABLE_PAGE_RECORD = OidPageidRecord(intField(), intField())

internal class TablePageRecordIteratorImpl(
    private val pageCache: PageCache,
    private val tableId: PageId): FullScanIteratorBase<OidPageidRecord>(TABLE_PAGE_RECORD::fromBytes) {

    private var currentCatalogPageId = -1

    init {
         currentCatalogPageId = tableId
        advance()
    }

    override fun advancePage(): CachedPage? {
        if (currentCatalogPageId == -1) {
            return null
        }
        val page = pageCache.get(currentCatalogPageId)
        return CatalogPageHeader.read(page).let {
            currentCatalogPageId = it.nextPageId
            page
        }
    }
}
