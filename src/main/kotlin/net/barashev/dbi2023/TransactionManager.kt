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

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

typealias TxnWriteTracker = (PageId, Boolean) -> Unit

class TransactionManager(private val scheduler: Scheduler, val logManager: LogManager): Scheduler by scheduler {
    private val txnWorkerScope = CoroutineScope(Executors.newCachedThreadPool().asCoroutineDispatcher())
    private val lastTxnId = AtomicInteger(0)
    private val txnCompletionScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())
    private val txnCompletionChannel = Channel<TransactionDescriptor>()
    override fun commit(txn: TransactionDescriptor): List<TransactionDescriptor> =
        scheduler.commit(txn).also {
            logManager.commit(txn)
            txnCompletionScope.launch { txnCompletionChannel.send(txn) }
        }

    override fun abort(txn: TransactionDescriptor): List<TransactionDescriptor> =
        scheduler.abort(txn).also {
            logManager.abort(txn)
            txnCompletionScope.launch { txnCompletionChannel.send(txn) }
        }

    fun txn(txnCode: suspend (PageCache, TransactionDescriptor) -> Unit) {
        val txnId = lastTxnId.incrementAndGet()
        txnWorkerScope.launch {
            val txnPageCache = TxnPageCacheImpl(logManager.pageCache, txnId, this@TransactionManager, txnCompletionChannel, logManager.start(txnId))
            try {
                txnCode(txnPageCache, txnId)
            } catch (ex: Exception) {
                abort(txnId)
            }
        }
    }
}

internal class TxnPageCacheImpl(
    private val proxiedCache: PageCache,
    private val txn: TransactionDescriptor,
    private val scheduler: Scheduler,
    private val txnCompletionChannel: Channel<TransactionDescriptor>,
    private val writeTracker: TxnWriteTracker
) : PageCache by proxiedCache {
    override fun get(pageId: PageId): CachedPage = tryRead(pageId) { proxiedCache.get(pageId) }

    override fun getAndPin(pageId: PageId): CachedPage = tryRead(pageId) { proxiedCache.getAndPin(pageId) }


    private fun tryRead(pageId: PageId, code: () -> CachedPage): CachedPage {
        while (true) {
            when (val read = scheduler.read(txn, pageId)) {
                is ReadOk -> {
                    return TxnPage(code(), this)
                }

                is ReadAbort -> {
                    scheduler.abort(txn)
                    throw TransactionException("Transaction $txn can't read page $pageId and has been aborted")
                }

                is ReadWait -> {
                    runBlocking {
                        while (true) {
                            val completedTxn = txnCompletionChannel.receive()
                            if (completedTxn == read.blockingTxn) {
                                break
                            }
                        }
                    }
                }
            }
        }
    }

    internal fun <T> tryWrite(pageId: PageId, code: () -> T): T {
        while (true) {
            when (val write = scheduler.write(txn, pageId)) {
                is WriteOk -> {
                    writeTracker(pageId, false)
                    return code().also {
                        writeTracker(pageId, true)
                    }
                }

                is WriteAbort -> {
                    scheduler.abort(txn)
                    throw TransactionException("Transaction $txn can't write page $pageId and has been aborted")
                }

                is WriteWait -> {
                    runBlocking {
                        while (true) {
                            val completedTxn = txnCompletionChannel.receive()
                            if (completedTxn == write.blockingTxn) {
                                break
                            }
                        }
                    }
                }
            }
        }
    }

}

internal class TxnPage(private val proxiedPage: CachedPage, private val txnCache: TxnPageCacheImpl): CachedPage by proxiedPage {
    override fun putRecord(recordData: ByteArray, recordId: RecordId): PutRecordResult =
        txnCache.tryWrite(proxiedPage.id) {
            proxiedPage.putRecord(recordData, recordId)
        }


    override fun deleteRecord(recordId: RecordId) =
        txnCache.tryWrite(proxiedPage.id) {
            proxiedPage.deleteRecord(recordId)
        }


    override fun clear() =
        txnCache.tryWrite(proxiedPage.id) {
            proxiedPage.clear()
        }
}