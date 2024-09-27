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
import net.barashev.dbi2023.fake.FakeHashTableBuilder
import net.barashev.dbi2023.fake.FakeIndexManager
import net.barashev.dbi2023.fake.FakeMergeSort
import net.barashev.dbi2023.fake.FakeNestedLoops
import org.slf4j.LoggerFactory
import kotlin.Result.Companion.failure
import kotlin.Result.Companion.success

/**
 * Feel free to change this code and add your own factories.
 */
fun initializeFactories(
    storage: Storage,
    cacheSize: Int = (System.getProperty("cache.size") ?: "100").toInt(),
    cacheImpl: String = System.getProperty("cache.impl", "fifo"),
    sortImpl: String = System.getProperty("sort.impl", "fake"),
    hashImpl: String = System.getProperty("hash.impl", "fake"),
    indexImpl: String = System.getProperty("index.impl", "fake"),
    optimizerImpl: String = System.getProperty("optimizer.impl", "fake"),
    walImpl: String = System.getProperty("wal.impl", "fake"),
): Pair<PageCache, StorageAccessManager> {
    LOGGER.debug("=".repeat(80))
    LOGGER.debug("Cache policy: $cacheImpl")
    LOGGER.debug("Cache size: $cacheSize")
    CacheManager.factory = { strg, size ->
        when (cacheImpl) {
            "none" -> NonePageCacheImpl(strg)
            "clock" -> ClockSweepPageCacheImpl(strg, size)
            "aging" -> AgingPageCacheImpl(strg, size)
            else -> FifoPageCacheImpl(strg, size)
        }
    }
    Operations.sortFactory = { strg, cache ->
        when (sortImpl) {
            "real" -> FastSortImpl(strg, cache)
            else -> SlowSortImpl(strg, cache)
        }
    }
    Operations.hashFactory = { storageAccessManager, pageCache ->
        when (hashImpl) {
            "real" -> TODO("Create your hash builder instance here")
            else -> FakeHashTableBuilder(storageAccessManager, pageCache)
        }
    }
    Operations.innerJoinFactory = { accessMethodManager, pageCache, joinAlgorithm ->
        when (joinAlgorithm) {
            JoinAlgorithm.NESTED_LOOPS -> success(FakeNestedLoops(accessMethodManager))
            JoinAlgorithm.HASH -> failure(NotImplementedError("Hash-Join not implemented yet"))
            JoinAlgorithm.MERGE -> success(SortMergeJoinImpl(accessMethodManager, pageCache))
        }
    }
    Indexes.indexFactory = { storageAccessManager, cache ->
        when (indexImpl) {
            "real" -> TODO("Create your index manager instance here")
            else -> FakeIndexManager(storageAccessManager, cache)
        }
    }
    walFactory = {
        when (walImpl) {
            "real" -> TODO("Create your WAL factory instance here")
            else -> FakeWAL()
        }
    }
    recoveryFactory = {
        when (walImpl) {
            "real" -> TODO("Create your recovery factory instance here")
            else -> NoRecovery()
        }
    }

    Optimizer.factory = { storageAccessManager, pageCache ->
        when (optimizerImpl) {
            "real" -> TODO("Create your optimizer here")
            else -> VoidOptimizer()
        }
    }

    val cache = CacheManager.factory(storage, cacheSize)
    val simpleAccessManager = SimpleStorageAccessManager(cache)

    return cache to simpleAccessManager
}

private val LOGGER = LoggerFactory.getLogger("Initialize")