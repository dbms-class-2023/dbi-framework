package net.barashev.dbi2023

import org.slf4j.LoggerFactory
import java.io.Closeable

/**
 * This is a simple implementation of a sort-merge join algorithm.
 * It sorts the input tables and writes them back to disk, and after that it merges the sorted tables, comparing
 * their top records.
 *
 * If for some join attribute value there is more than one matching pair, it runs a nested loop over the matching tuples.
 * With some chances the nested loop accesses the pages that are already in the page cache, so it is supposed to be cheap.
 */
class SortMergeJoinImpl(private val storageAccessManager: StorageAccessManager, private val cache: PageCache): InnerJoin {
    private val closeables = mutableListOf<AutoCloseable>()

    override fun <T : Comparable<T>> join(leftTable: JoinOperand<T>, rightTable: JoinOperand<T>): JoinOutput {
        cache.stats.reset()
        // Sort the inputs and write the intermediate sorted tables.
        val sortedLeftTable = Operations.sortFactory(storageAccessManager, cache).sort(leftTable.tableName, leftTable.joinAttribute)
        val sortedRightTable = Operations.sortFactory(storageAccessManager, cache).sort(rightTable.tableName, rightTable.joinAttribute)
        // Remove the intermediate tables.
        closeables.add(Closeable {
            storageAccessManager.deleteTable(sortedLeftTable)
            storageAccessManager.deleteTable(sortedRightTable)
        })
        LOGGER.debug("""Stats after sorting: 
            |Cache
            |------------------
            |${cache.stats}""".trimMargin())
        cache.stats.reset()
        // Create buffered iterators over the sorted tables.
        val leftRecords = BufferedRecordIterator(storageAccessManager, cache, 10, leftTable, sortedLeftTable)
        val rightRecords = BufferedRecordIterator(storageAccessManager, cache, 10, rightTable, sortedRightTable)
        // Don't forget to release the buffered pages when we are done.
        closeables.add(leftRecords)
        closeables.add(rightRecords)

        LOGGER.debug("""Stats after joining: 
            |Cache
            |------------------
            |${cache.stats}""".trimMargin())
        return OutputIterator(leftRecords, rightRecords)
    }

    override fun close() {
        closeables.forEach { it.close() }
    }
}

/**
 * This output iterator lazily compares the top records and advances the left or right iterator depending
 * on what join attribute value is lesser.
 */
class OutputIterator<T: Comparable<T>>(
    private val leftRecords: BufferedRecordIterator<T>,
    private val rightRecords: BufferedRecordIterator<T>)
    : JoinOutput {

    private var rightIterator: Iterator<Pair<T, ByteArray>>? = null
    private var currentPair: Pair<ByteArray, ByteArray>? = null

    init {
        advance()
    }

    private fun advance() {
        // We will loop until we find a matching pair or run out of some input.
        while (currentPair == null) {
            rightIterator?.let {
                // If there is a right iterator then at some moment we found matching records and started iterating
                // over the right table records.
                if (it.hasNext()) {
                    // If the left top record and the current record from the iterator match then we stop advancing.
                    val leftTopRecord = leftRecords.topRecord()
                    val nextRightRecord = it.next()
                    if (nextRightRecord.first == leftTopRecord.first) {
                        currentPair = leftTopRecord.second to nextRightRecord.second
                        return
                    }
                    // If they don't match or if we reached the end of the iterator, we need to pull the left records
                    // and nullify the iterator.
                }
                currentPair = null
                leftRecords.pull()
                rightIterator = null
            }

            if (leftRecords.hasNext() && rightRecords.hasNext()) {
                // Let's compare the top records from the sorted tables.
                val leftValue = leftRecords.topRecord().first
                val rightValue = rightRecords.topRecord().first
                if (leftValue < rightValue) {
                    // Left iterator needs to advance.
                    currentPair = null
                    leftRecords.pull()
                } else if (leftValue > rightValue) {
                    // Right iterator needs to advance.
                    currentPair = null
                    rightRecords.pull()
                }
                else {
                    // Great, we have found a match on some value V. Now we want to scan through all "right" records with the same
                    // matching value V. For this purpose we create an additional iterator that is positioned on the current
                    // top record from the "right" iterator.
                    rightIterator = rightRecords.iterateFromTop()
                }
            } else {
                // We ran out of one of the tables, no more matches expected.
                currentPair = null
                break
            }
        }
    }
    override fun hasNext(): Boolean = currentPair != null

    override fun next(): Pair<ByteArray, ByteArray>  = currentPair!!.also{
        currentPair = null
        advance()
    }

    override fun close() {
    }
}

/**
 * This class encapsulates the algorithm of scanning through a sorted join table, so that we could
 * find all matching pairs easily.
 *
 * In this class, the "top" record is the current record of the main scan.
 */
class BufferedRecordIterator<T>(
    private val storageAccessManager: StorageAccessManager,
    private val cache: PageCache,
    bufferSize: Int,
    private val joinOperand: JoinOperand<T>,
    private val sortedTableName: String): AutoCloseable {

    // The main scan over the sorted table, one full pass.
    private val scan = storageAccessManager.createFullScan(sortedTableName).pages().iterator()
    // A queue of the scanned pages. The first page in the queue is the one that contains the "top" record.
    private val pageChunk = mutableListOf<CachedPage>()
    // Position of the top record in the list of all records from the first buffered page.
    private var topRecordIdx: Int = 0
    // All records from the first buffered page.
    private var topRecords = mutableListOf<ByteArray>()
    // Identifier of the first buffered page.
    private val topPageId get() = pageChunk.first().id

    init {
        // Initially we buffer up to bufferSize pages.
        repeat(bufferSize) {
            if (!loadPage()) {
                return@repeat
            }
        }
    }

    // We have more records if there is at least one buffered page and the top record is not the last one.
    fun hasNext(): Boolean = pageChunk.isNotEmpty() && topRecordIdx < topRecords.size

    /**
     * Loads the next page into the buffer and replaces the list of records.
     */
    fun loadPage(): Boolean {
        if (scan.hasNext()) {
            scan.next().let {
                pageChunk.add(cache.getAndPin(it.id))
            }
        }
        if (pageChunk.isEmpty()) {
            return false
        }
        topRecords.clear()
        topRecords.addAll(pageChunk.first().allRecords().map { it.value.bytes })
        if (topRecords.isEmpty()) {
            return false
        }
        topRecordIdx = 0
        return true
    }

    /**
     * Removes the first page from the buffer and unpins it.
     */
    fun evictPage() {
        if (pageChunk.isNotEmpty()) {
            pageChunk.removeFirst().close()
        }
    }

    /**
     * The top record, a pair of a join attribute value and raw bytes.
     */
    fun topRecord(): Pair<T, ByteArray> = topRecords[topRecordIdx].let { joinOperand.joinAttribute.apply(it) to it }

    /**
     * Pulls the main iterator, that is, selects the next top record. If the current top page is scanned until the last record,
     * evicts it and loads a new one.
     */
    fun pull(): Boolean {
        topRecordIdx++
        if (topRecordIdx == topRecords.size) {
            evictPage()
            return loadPage()
        } else {
            return true
        }
    }

    /**
     * Creates an auxiliary iterator over the sorted records, starting from the current top record.
     * This is used when we find matching left and right records and want to scan through all right records
     * with the same join attribute value.
     */
    fun iterateFromTop(): Iterator<Pair<T, ByteArray>> =
        storageAccessManager.createFullScan(sortedTableName).startAt(topPageId).records {
            joinOperand.joinAttribute.apply(it) to it
        }.iterator().drop(topRecordIdx)

    override fun close() {
        while (pageChunk.isNotEmpty()) {
            evictPage()
        }
    }
}

/**
 * Skips the first num elements in the iterator.
 */
private fun <T> Iterator<Pair<T, ByteArray>>.drop(num: Int): Iterator<Pair<T, ByteArray>> {
    var count = num
    while (hasNext() && count > 0) {
        count--
        next()
    }
    return this
}


private val LOGGER = LoggerFactory.getLogger("Join.SortMerge")