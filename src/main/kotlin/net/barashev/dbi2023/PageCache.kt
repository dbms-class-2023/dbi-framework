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

data class CachedPageUsage(
    val accessCount: Int,
    val lastAccessTs: Long,
)

/**
 * Enhances DiskPage interface with "dirty" flag and usage counts.
 */
interface CachedPage : AutoCloseable, DiskPage {
    val isDirty: Boolean
    val usage: CachedPageUsage
}

/**
 * This interface represents a buffer cache which buffers disk pages in RAM.
 * The following features are supported:
 * - loading a single page or a sequence of pages into the cache without pinning
 * - pinning a particular page, which guarantees that it will not be evicted
 * - flushing the whole cache to the storage
 */
interface PageCache {
    /**
     * Loads a sequence of pages into the cache without pinning, e.g. for read-ahead purposes.
     *
     */
    fun load(startPageId: PageId, pageCount: Int = 1)

    /**
     * Fetches a single page into the cache.
     */
    fun get(pageId: PageId): CachedPage

    /**
     * Fetches a single page into the cache and pins it. It is client's responsibility to close CachedPage instances
     * returned from this method after using.
     */
    fun getAndPin(pageId: PageId): CachedPage

    /**
     * Flushes the cached pages to the disk storage.
     */
    fun flush()

    /**
     * Returns this cache hit/miss statistics.
     */
    val stats: PageCacheStats

    /**
     * Returns the capacity (maximum size) of this cache in pages
     */
    val capacity: Int
}

interface PageCacheStats {
    val cacheHit: Int
    val cacheMiss: Int
}

object CacheManager {
    var factory: (Storage, Int) -> PageCache = { storage, size ->
        SimplePageCacheImpl(storage, size)
    }
}
