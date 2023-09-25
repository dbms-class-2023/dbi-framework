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

import kotlin.math.round

internal data class StatsImpl(var cacheHitCount: Int = 0, var cacheMissCount: Int = 0): PageCacheStats {
    override val cacheHit: Int
        get() = cacheHitCount
    override val cacheMiss: Int
        get() = cacheMissCount

    override fun reset() {
        cacheHitCount = 0
        cacheMissCount = 0
    }

    val addTracker = mutableMapOf<PageId, Int>()
    override fun toString(): String {
        return """Cache stats:
            hit ratio=${round(100.0*cacheHitCount/(cacheHitCount+cacheMissCount))} hits=$cacheHitCount misses=$cacheMissCount
            pages loaded >10 times:
            ${addTracker.filterValues { it > 10 }})"""
    }
}

internal open class CachedPageImpl(
    internal val diskPage: DiskPage,
    var pinCount: Int = 1): CachedPage, DiskPage by diskPage {

    internal var _isDirty = false
    override val isDirty: Boolean get() = _isDirty

    override var usage = CachedPageUsage(0, System.currentTimeMillis())
    override fun putRecord(recordData: ByteArray, recordId: RecordId): PutRecordResult = diskPage.putRecord(recordData, recordId).also {
        _isDirty = true
    }

    override fun deleteRecord(recordId: RecordId) = diskPage.deleteRecord(recordId).also {
        _isDirty = true
    }

    override fun close() {
        if (pinCount > 0) {
            pinCount -= 1
        }
    }

    internal fun incrementUsage() {
        usage = CachedPageUsage(usage.accessCount + 1, System.currentTimeMillis())
    }
}

/**
 * This class implements a simple FIFO-like page cache. It is open, that is, allows for subclassing and overriding
 * some methods.
 */
open class SimplePageCacheImpl(internal val storage: Storage, private val maxCacheSize: Int = -1): PageCache {
    private val statsImpl = StatsImpl()
    override val stats: PageCacheStats get() = statsImpl
    internal val cacheArray = mutableListOf<CachedPageImpl>()
    internal val cache get() = cacheArray.associateBy { it.id }

    override val capacity = maxCacheSize

    override fun load(startPageId: PageId, pageCount: Int) = doLoad(startPageId, pageCount, this::doAddPage)


    internal fun doLoad(startPageId: PageId, pageCount: Int, addPage: (page: DiskPage) -> CachedPageImpl) {
        storage.bulkRead(startPageId, pageCount) { diskPage ->
            val cachedPage = cache[diskPage.id] ?: addPage(diskPage)
            // We do not record cache hit or cache miss because load is a bulk operation and most likely it loads the
            // pages which are not yet cached. Recording cache miss will skew the statistics.
            onPageRequest(cachedPage, null)
        }
    }

    override fun get(pageId: PageId): CachedPage = doGetAndPin(
        pageId,
        this::doAddPage,
        0
    )

    override fun getAndPin(pageId: PageId): CachedPage = doGetAndPin(
        pageId,
        this::doAddPage
    )

    internal fun doGetAndPin(pageId: PageId, addPage: (page: DiskPage) -> CachedPageImpl, pinIncrement: Int = 1): CachedPageImpl {
        var cacheHit = true
        return cache.getOrElse(pageId) {
            cacheHit = false
            addPage(storage.read(pageId))
        }.also {
            onPageRequest(it, isCacheHit = cacheHit)
            it.pinCount += pinIncrement
        }
    }

    private fun doAddPage(page: DiskPage): CachedPageImpl {
        val result = CachedPageImpl(page, 0)
        if (cache.size == maxCacheSize) {
            swap(getEvictCandidate(), result)
        } else {
            synchronized(cacheArray) {
                this.cacheArray.add(result)
            }
        }
        statsImpl.addTracker[page.id] = (statsImpl.addTracker[page.id] ?: 0) + 1
        return result
    }

  override fun flush() {
        cache.forEach { (_, cachedPage) -> cachedPage.write() }
    }

    internal fun swap(victim: CachedPageImpl, newPage: CachedPageImpl) {
        victim.write()
        synchronized(cacheArray) { doSwap(victim, newPage) }
    }

    // By default we place the new page at the same index where the victim used to be.
    // Override this to implement your own swap policy.
    internal open fun doSwap(victim: CachedPageImpl, newPage: CachedPageImpl) {
        val idx = cacheArray.indexOf(victim)
        cacheArray[idx] = newPage
    }

    private fun recordCacheHit(isCacheHit: Boolean) =
        if (isCacheHit) statsImpl.cacheHitCount += 1 else statsImpl.cacheMissCount += 1

    internal fun CachedPageImpl.write() {
        if (this.isDirty) {
            storage.write(this.diskPage)
        }
        this._isDirty = false
    }
    // -------------------------------------------------------------------------------------------------------------
    // Override these functions to implement custom page replacement policy
    internal open fun getEvictCandidate(): CachedPageImpl {
        return cache.values.firstOrNull {
            it.pinCount == 0
        } ?: throw IllegalStateException("All pages are pinned, there is no victim for eviction")
    }

    internal open fun onPageRequest(page: CachedPageImpl, isCacheHit: Boolean?) {
        isCacheHit?.let(this::recordCacheHit)
        page.incrementUsage()
    }
}

internal class NoCachedPageImpl(private val storage: Storage, diskPage: DiskPage) : CachedPageImpl(diskPage, 0) {
    override fun close() {
        if (this.isDirty) {
            storage.write(this.diskPage)
        }
        this._isDirty = false
        super.close()
    }
}

class NonePageCacheImpl(private val storage: Storage): PageCache {
    override fun load(startPageId: PageId, pageCount: Int) {
        storage.bulkRead(startPageId, pageCount) {
            // We do nothing with the pages that we read
        }
    }

    override fun get(pageId: PageId): CachedPage =
        NoCachedPageImpl(storage, storage.read(pageId))


    override fun getAndPin(pageId: PageId): CachedPage = get(pageId)

    override fun flush() {
        // No cache -- no flush
    }

    private val statsImpl = StatsImpl()
    override val stats: PageCacheStats get() = statsImpl
    override val capacity: Int = Int.MAX_VALUE
}

class FifoPageCacheImpl(storage: Storage, maxCacheSize: Int): SimplePageCacheImpl(storage, maxCacheSize) {
    override fun doSwap(victim: CachedPageImpl, newPage: CachedPageImpl) {
        cacheArray.remove(victim)
        cacheArray.add(newPage)
    }
}
