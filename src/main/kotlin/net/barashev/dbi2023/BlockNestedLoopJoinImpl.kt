package net.barashev.dbi2023

import java.util.function.Function

/**
 * Ilia Kholkin
 */
class BlockNestedLoopJoinImpl(
    private val storageAccessManager: StorageAccessManager, private val cache: PageCache, private val cacheSize: Int
) : InnerJoin {
    override fun <T : Comparable<T>> join(leftTable: JoinOperand<T>, rightTable: JoinOperand<T>): JoinOutput {
        val leftPageCount = storageAccessManager.pageCount(leftTable.tableName)
        val chunkSize = cacheSize - 1

        val outerPages = storageAccessManager.createFullScan(leftTable.tableName).pages().iterator()
        val innerFullScan = storageAccessManager.createFullScan(rightTable.tableName)

        return object : JoinOutput {
            // number of chunks of pages left to read
            var outerChunksLeft = (leftPageCount + chunkSize - 1) / chunkSize - 1

            // outer table variables down to current outer record
            var outerChunk: List<CachedPage> = readNextChunk()
            var outerRecordIterator: Iterator<ByteArray> = getRecordIterator(outerChunk)
            var outerRecord: ByteArray? = if (outerRecordIterator.hasNext()) outerRecordIterator.next() else null

            // inner table variables down to an iterator over records of the current page
            var innerPageIterator: Iterator<CachedPage> = innerFullScan.pages().iterator()
            var innerPage: CachedPage = innerPageIterator.next()
            var innerRecordIterator: Iterator<ByteArray> = getRecordIterator(listOf(innerPage))

            // last satisfying pair
            var currentPair: Pair<ByteArray, ByteArray>? = getNextSatisfyingPair()

            override fun hasNext(): Boolean {
                return currentPair != null
            }

            override fun next(): Pair<ByteArray, ByteArray> {
                return currentPair!!.also {
                    currentPair = getNextSatisfyingPair()
                }
            }

            override fun close() = closeCurrentPages()

            private fun makeOuterStep(shouldGoToNextChunk: Boolean = true): ByteArray? {
                if (isEndOfOuterTable() && shouldGoToNextChunk) {
                    outerRecord = null
                    return null
                }
                if (isEndOfChunk()) {
                    if (shouldGoToNextChunk) {
                        outerChunksLeft--
                        closeCurrentChunk()
                        outerChunk = readNextChunk()
                    }
                    outerRecordIterator = getRecordIterator(outerChunk)
                }
                return outerRecordIterator.next().also { outerRecord = it }
            }

            private fun getNextPairToCheck(): Pair<ByteArray, ByteArray>? {
                if (isEndOfInnerTable() && isEndOfOuterTable()) {
                    return null
                }
                if (isEndOfInnerPage()) {
                    if (isEndOfChunk()) {
                        makeOuterStep(shouldGoToNextChunk = isEndOfInnerTable())
                        moveToNextInnerPage()
                    } else {
                        makeOuterStep()
                        innerRecordIterator = getRecordIterator(listOf(innerPage))
                    }
                }
                return outerRecord!! to innerRecordIterator.next()
            }

            private fun getNextSatisfyingPair(): Pair<ByteArray, ByteArray>? {
                var nextSatisfyingPair: Pair<ByteArray, ByteArray>?
                do {
                    nextSatisfyingPair = getNextPairToCheck()
                } while (nextSatisfyingPair != null && !satisfies(nextSatisfyingPair.first, nextSatisfyingPair.second))
                return nextSatisfyingPair
            }

            private fun satisfies(outerRecord: ByteArray, innerRecord: ByteArray) =
                leftTable.joinAttribute.apply(outerRecord) == rightTable.joinAttribute.apply(innerRecord)

            private fun getRecordIterator(pages: List<CachedPage>): Iterator<ByteArray> =
                pages.map { page ->
                    page.allRecords().map { (_, getRecordResult) ->
                        if (getRecordResult.isOk) {
                            getRecordResult.bytes
                        } else {
                            null
                        }
                    }.filterNotNull()
                }.flatten().iterator()

            private fun readNextChunk(): List<CachedPage> = buildList {
                repeat(chunkSize) {
                    if (!outerPages.hasNext()) {
                        return@buildList
                    }
                    add(outerPages.next().let { page -> cache.get(page.id) })
                }
            }

            private fun moveToNextInnerPage() {
                if (isEndOfInnerTable()) {
                    innerPageIterator = innerFullScan.pages().iterator()
                }
                innerPage.close()
                innerPage = innerPageIterator.next().let { page -> cache.getAndPin(page.id) }
                innerRecordIterator = getRecordIterator(listOf(innerPage))
            }

            private fun closeCurrentChunk() = outerChunk.forEach { it.close() }

            private fun closeCurrentPages() {
                closeCurrentChunk()
                innerPage.close()
            }

            private fun isEndOfChunk() = !outerRecordIterator.hasNext()

            private fun isEndOfOuterTable() = isEndOfChunk() && outerChunksLeft == 0

            private fun isEndOfInnerPage() = !innerRecordIterator.hasNext()

            private fun isEndOfInnerTable() = isEndOfInnerPage() && !innerPageIterator.hasNext()
        }
    }

    override fun close() {}
}
