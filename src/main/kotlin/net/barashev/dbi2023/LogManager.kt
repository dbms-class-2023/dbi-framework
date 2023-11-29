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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedDeque

/**
 * Write-ahead log interface. It is responsible for maintaining undo- or redo- log with information sufficient
 * for restoring a database in case of failures.
 */
interface WAL {
    /**
     * This is called when a transaction is started, before it makes its first action.
     */
    fun transactionStarted(txn: TransactionDescriptor)

    /**
     * This is called when a transaction is about to modify a page, but before a modification really takes place.
     */
    fun beforePageWrite(txn: TransactionDescriptor, page: CachedPage)

    /**
     * This is called when a transaction invoked a function which could modify a page, no matter if any modification
     * actually took place.
     */
    fun afterPageWrite(txn: TransactionDescriptor, page: CachedPage)

    /**
     * This is called when a transaction was aborted due to either explicit abort call or due to error.
     * Modified pages in cache by this moment have already been reset (that is, modifications made by this transactions
     * have been reverted). Transactions which might be waiting for txn has not been
     * notified yet.
     */
    fun transactionAborted(txn: TransactionDescriptor, cache: PageCache, modifiedPages: Set<PageId>)

    /**
     * This is called when a transaction was committed. Modified pages in cache by this moment are not yet synced with
     * the disk, but may be synced at any moment later. Transactions which might be waiting for txn has not been
     * notified yet.
     */
    fun transactionCommitted(txn: TransactionDescriptor, cache: PageCache, modifiedPages: Set<PageId>)
}

/**
 * Factory for creating WAL instances. It is parameterized with a separate storage instance for WAL records.
 *
 * Please set your own walFactory instance from your own code.
 */
var walFactory: (walStorage: Storage) -> WAL = {
    TODO("Please DONT write your code here. If you need to create your own factory, assign walFactory from your own code: walFactory={ -> }")
}

/**
 * Recovery process interface.
 */
interface Recovery {
    /**
     * Runs recovery process with the WAL log stored on walStorage and database being recovered on mainStorage.
     * It is assumed that mainStorage state corresponds to the beginning of the WAL history.
     */
    fun run(walStorage: Storage, mainStorage: Storage)
}

/**
 * Factory function for creating recovery instances. Please set your own factory from your own code.
 */
var recoveryFactory: () -> Recovery = {
    TODO("Please DONT write your code here. If you need to create your own factory, assign recoveryFactory from your own code: recoveryFactory={ -> }")
}


/**
 * Log Manager responsibility is to keep track of all started and completed transactions and pages which they modify.
 * It uses a WAL instance to actually build the log.
 */
class LogManager(realStorage: Storage, cacheSize: Int, private val wal: WAL, useRevertableStorage: Boolean = true) {
    val pageCache: PageCache

    private val txnWrites = ConcurrentHashMap<TransactionDescriptor, MutableSet<PageId>>()
    private val allWrites = ConcurrentHashMap<PageId, Boolean>()//mutableMapOf<PageId, Boolean>()

    init {
        val loggingStorage = if (useRevertableStorage) RevertableStorage(realStorage, this::isTxnModified) else realStorage
        pageCache = CacheManager.factory(loggingStorage, cacheSize)
    }

    fun start(txn: TransactionDescriptor): TxnWriteTracker {
        wal.transactionStarted(txn)
        return { pageId, writeDone ->
            if (!writeDone) {
                wal.beforePageWrite(txn, pageCache.get(pageId))
            }
            txnWrites.getOrPut(txn) { mutableSetOf() }.add(pageId)
            allWrites[pageId] = true
            if (writeDone) {
                wal.afterPageWrite(txn, pageCache.get(pageId))
            }
        }
    }

    fun abort(txn: TransactionDescriptor) {
        (txnWrites.remove(txn) ?: setOf()).also {modifiedPages ->
            allWrites.keys.removeAll(modifiedPages)
            modifiedPages.forEach { pageId ->
                pageCache.get(pageId).let {
                    it.reset()
                }
            }
            wal.transactionAborted(txn, pageCache, modifiedPages)
        }
    }

    fun commit(txn: TransactionDescriptor) {
        txnWrites.remove(txn)?.let {modifiedPages ->
            allWrites.keys.removeAll(modifiedPages)
            wal.transactionCommitted(txn, pageCache, modifiedPages)
        }

    }

    private fun isTxnModified(pageId: PageId) = allWrites.containsKey(pageId)
}

// This storage implementation will ignore disk page write calls related to pages modified by any running transaction.
// This is just to make sure that modified pages will not accidentally reach the disk because of cache flushing.
internal class RevertableStorage(private val realStorage: Storage, private val isTxnModified: (PageId)->Boolean): Storage by realStorage {
    override fun write(page: DiskPage) {
        if (!isTxnModified(page.id)) {
            realStorage.write(page)
        }
    }
}

internal class FakeWAL: WAL {
    private val log = ConcurrentLinkedDeque<String>()
    override fun transactionStarted(txn: TransactionDescriptor) {
        log.add("<START T$txn>")
    }

    override fun beforePageWrite(txn: TransactionDescriptor, page: CachedPage) {
        log.add("<BEFORE T$txn, ${page.id}>")
    }

    override fun afterPageWrite(txn: TransactionDescriptor, page: CachedPage) {
        log.add("<AFTER T$txn, ${page.id}>")
    }

    override fun transactionAborted(txn: TransactionDescriptor, cache: PageCache, modifiedPages: Set<PageId>) {
        log.add("<ABORT T$txn (modified ${modifiedPages.size} pages)>")
        println("added abort to log")
    }

    override fun transactionCommitted(txn: TransactionDescriptor, cache: PageCache, modifiedPages: Set<PageId>) {
        log.add("<COMMIT T$txn (modified ${modifiedPages.size} pages)>")
    }

    override fun toString() = log.joinToString(separator = "\n")
}

internal class NoRecovery: Recovery {
    override fun run(walStorage: Storage, mainStorage: Storage) {
    }
}
