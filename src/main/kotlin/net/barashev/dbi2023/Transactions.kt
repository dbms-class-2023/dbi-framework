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

/**
 * Transaction descriptor is just an integer number. One can assume that it is a monotonically increasing sequence
 * and it can be used as a logical timestamp if needed.
 */
typealias TransactionDescriptor = Int

sealed class ReadResult

/**
 * The requested read is OK. However, in some cases, e.g. when reading pages in MVCC protocol,
 * another page may be suggested as a replacement (e.g. some previous version of the same page)
 */
data class ReadOk(val page: PageId): ReadResult()

/**
 * The requested read is not OK and transaction must be aborted.
 */
data class ReadAbort(val reason: String): ReadResult()

/**
 * The requested read shall wait until the transaction `blockingTxn` commits or aborts.
 */
data class ReadWait(val blockingTxn: TransactionDescriptor): ReadResult()


sealed class WriteResult

/**
 * The requested write is ok. If write produces a new version of a page, it shall report it back using `versionWriteCallback`
 */
data class WriteOk(val versionWriteCallback: (TransactionDescriptor, PageId) -> Unit): WriteResult()

/**
 * The requested write is not OK and the transaction must be aborted.
 */
data class WriteAbort(val reason: String): WriteResult()

/**
 * The requested write shall wait until the transaction `blockingTxn` commits or aborts.
 */
data  class WriteWait(val blockingTxn: TransactionDescriptor): WriteResult()

/**
 * This is a transaction action scheduler which is responsible for building a serializable schedule.
 * It takes transaction actions on pages and commit/abort messages as the input and produces output which
 * is used by the transaction manager to decide what shall be done with the transactions.
 *
 * For read/write calls the possible outputs are enumerated in sealed classes ReadResult/WriteResult.
 * For commit/abort calls the output is a list of transactions which have been waiting and shall be resumed.
 *
 * Implementation may assume that access to all methods, including version write callbacks, is single-threaded.
 */
interface Scheduler {
    /**
     * Transaction `txn` wants to read a page `page`.
     */
    fun read(txn: TransactionDescriptor, page: PageId): ReadResult

    /**
     * Transaction `txn` wants to write a page `page`.
     */
    fun write(txn: TransactionDescriptor, page: PageId): WriteResult

    /**
     * Transaction `txn` commits.
     *
     * @return the list of transactions which have been waiting for `txn` to complete.
     */
    fun commit(txn: TransactionDescriptor): List<TransactionDescriptor>

    /**
     * Transaction `txn` aborts.
     *
     * @return the list of transactions which have been waiting for `txn` to complete.
     */
    fun abort(txn: TransactionDescriptor): List<TransactionDescriptor>
}

enum class SchedulerProtocol {
    TWO_PHASE_LOCKING, TIMESTAMP_ORDERING, MVCC
}

var schedulerFactory: (SchedulerProtocol) -> Scheduler = { FakeScheduler() }

class FakeScheduler: Scheduler {
    override fun read(txn: TransactionDescriptor, page: PageId): ReadResult {
        return ReadOk(page)
    }

    override fun write(txn: TransactionDescriptor, page: PageId): WriteResult {
        return WriteOk { _, _ -> }
    }

    override fun commit(txn: TransactionDescriptor): List<TransactionDescriptor> {
        return emptyList()
    }

    override fun abort(txn: TransactionDescriptor): List<TransactionDescriptor> {
        return emptyList()
    }

}

class TransactionException(msg: String): RuntimeException(msg)