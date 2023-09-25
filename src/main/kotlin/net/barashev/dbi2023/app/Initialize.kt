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

package net.barashev.dbi2023.app

import net.barashev.dbi2023.*

/**
 * Feel free to change this code and add your own factories.
 */
fun initializeFactories(
    storage: Storage,
    cacheSize: Int = (System.getProperty("cache.size") ?: "100").toInt(),
    cacheImpl: String = System.getProperty("cache.impl", "fifo")
    ): Pair<PageCache, StorageAccessManager> {
    println("=".repeat(80))
    println("Cache factory: ${System.getProperty("cache.impl")}")
    println("Cache size: $cacheSize")
    CacheManager.factory = { strg, size ->
        when (cacheImpl) {
            "none" -> NonePageCacheImpl(strg)
            else -> FifoPageCacheImpl(strg, size)
        }
    }

    val cache = CacheManager.factory(storage, cacheSize)
    val simpleAccessManager = SimpleStorageAccessManager(cache)

    return cache to simpleAccessManager
}