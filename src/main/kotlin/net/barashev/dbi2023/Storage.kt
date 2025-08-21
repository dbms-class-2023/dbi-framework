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

import java.util.function.Consumer

typealias PageId = Int
typealias RecordId = Int

/**
 * The result of the record fetch request from a disk page. If record exists and is not deleted, the `bytes` array
 * will be filled with the record bytes, suitable for passing to record parsers.
 */
data class GetRecordResult(val bytes: ByteArray, val isDeleted: Boolean = false, val isOutOfRange: Boolean = false) {
    val isOk = !isDeleted && !isOutOfRange
}

/**
 * The result of adding a record to a disk page. If put operation completes successfully, recordId field will
 * be set to the id of the new record. Otherwise, boolean flags indicating the failure reason will be set.
 */
data class PutRecordResult(val recordId: RecordId, val isOutOfSpace: Boolean = false, val isOutOfRange: Boolean = false) {
    val isOk = !isOutOfSpace && !isOutOfRange
}

/**
 * A toy disk page which allows for storing indexed records.
 *
 * A record is just a byte array, and its interpretation is left on the clients of this API. Record identifiers
 * are zero-based index values. It is assumed that all records in a single page occupy thw whole range of
 * index values from 0 until the maximum record id at that page without gaps.
 *
 * Record identifiers are not guaranteed to be permanent, and implementation may or may not change then as
 * records are added or deleted. In particular, the implementation is free to choose how to handle record removals,
 * and may either remove it completely and thus change the ids of some records, or to put a tombstone instead.
 */
interface DiskPage {

    /** Identifier of this page */
    val id: PageId

    /** The whole byte array with the page data */
    val rawBytes: ByteArray

    val headerSize: Int get() = Int.SIZE_BYTES
    /**
     * The number of bytes on this page, which are not occupied by records or auxiliary data. The reported
     * free space may or may not be sufficient for adding new records, depending on how records and auxiliary data
     * structures grow internally.
     */
    val freeSpace: Int

    fun putHeader(header: ByteArray)
    fun getHeader(): ByteArray
    /**
     * Puts the passed record into the page. Record id is supposed to be in the range [0..recordCount]. Special value
     * -1 is accepted too, and is equivalent to recordCount.
     * If record id is equal to recordCount, a new record is added, otherwise the existing record is updated.
     * Boolean flags in the result indicate if put completed successfully or if there were any issues.
     */
    fun putRecord(recordData: ByteArray, recordId: RecordId = -1): PutRecordResult

    /**
     * Returns a record by its id. In the result object a boolean flag isOk indicates whether the operation was successful.
     * If isOk is false, other boolean flags indicate what is the issue. In particular, a record may exist but may be
     * logically deleted, and in this case isDeleted flag is set to true.
     */
    fun getRecord(recordId: RecordId): GetRecordResult

    /**
     * Deletes a record with the given id if it exists
     */
    fun deleteRecord(recordId: RecordId)

    /**
     * Returns all records. Records which are logically deleted are returned as well.
     */
    fun allRecords(): Map<RecordId, GetRecordResult>

    /**
     * Clears the page, that is, removes all the records.
     */
    fun clear()

    /**
     * If this page is backed by some prototype (e.g. this is a cached page, that corresponds to some disk page
     * on the storage), copies the data from the prototype into this page.
     */
    fun reset()
}

/**
 * An abstraction of a bulk write, when many pages are written to the storage with a single physical operation.
 * If the storage is a HDD disk, a bulk write may correspond to positioning the head to some specific track and sequentially
 * writing all the pages on that track.
 *
 * Bulk write object needs to be closed when writing is completed.
 */
interface BulkWrite: AutoCloseable {
    /**
     * Identifier of the page where this bulk write started writing pages
     */
    val startPageId: PageId

    /**
     * Writes the next input page and returns a new instance of a page corresponding to the location where
     * it was written. For instance, the returned page may have different id and the method caller will want to update
     * its page mappings, if any.
     *
     * @throws StorageException if called when this bulk write is already closed.
     */
    fun write(inPage: DiskPage): DiskPage
}

/**
 * Storage abstraction, which allows for random and sequential reads and writes.
 */
interface Storage: AutoCloseable {
    /**
     * Reads a single page with the given page ID.
     */
    fun read(pageId: PageId): DiskPage

    /**
     * Reads a sequence of numPages pages starting from the given pageid. Calls a reader object for each page.
     *
     * It depends on the implementation if this method is blocking, that is, if it returns before all pages are
     * read and passed to the reader.
     *
     * In any case, pages are fed to the reader serially, that is, every next call to the reader's accept() method waits
     * until the previous call completes.
     */
    fun bulkRead(startPageId: PageId, numPages: Int = 1, reader: Consumer<DiskPage>)

    /**
     * Writes a single page to the storage. The page location on the storage is defined by its id.
     */
    fun write(page: DiskPage)

    /**
     * Creates an implementation of a bulk write at the location defined by startPageId.
     *
     * It depends on the implementation if the returned bulk writes blocks any other write or read operations
     * until it is closed.
     */
    fun bulkWrite(startPageId: PageId): BulkWrite

    /**
     * The value of the storage access cost accumulated since the moment when the storage object was initialized.
     */
    val totalAccessCost: Double
}