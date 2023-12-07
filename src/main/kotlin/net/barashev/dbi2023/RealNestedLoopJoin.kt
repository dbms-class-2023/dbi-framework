package net.barashev.dbi2023

/**
 * Egor Shibaev
 */
class RealNestedLoopJoin(
    private val storageAccessManager: StorageAccessManager, private val cache: PageCache
) : InnerJoin {
    override fun <T : Comparable<T>> join(leftTable: JoinOperand<T>, rightTable: JoinOperand<T>): JoinOutput {
        val (leftTableName, joinAttr1) = leftTable
        val (rightTableName, joinAttr2) = rightTable

        val iterator = iterator {
            storageAccessManager.createFullScan(leftTableName).pages().chunkedSequence(cache.capacity - 1)
                .forEach { pages ->

                    storageAccessManager.createFullScan(rightTableName).pages().forEach { innerPage ->

                        innerPage.allRecords().forEach { innerRecord ->
                            pages.forEach { outerPage ->
                                outerPage.allRecords().forEach { outerRecord ->
                                    if (innerRecord.value.isOk &&
                                        outerRecord.value.isOk &&
                                        joinAttr1.apply(outerRecord.value.bytes) == joinAttr2.apply(innerRecord.value.bytes)
                                    ) {
                                        yield(Pair(outerRecord.value.bytes, innerRecord.value.bytes))
                                    }
                                }
                            }
                        }

                        innerPage.close()
                    }

                    pages.forEach { page ->
                        page.close()
                    }
                }
        }

        return object : JoinOutput {
            private val iterator = iterator
            override fun hasNext(): Boolean = iterator.hasNext()

            override fun next(): Pair<ByteArray, ByteArray> = iterator.next()

            override fun close() {
            }

        }
    }


}

fun <T> Iterable<T>.chunkedSequence(size: Int): Sequence<List<T>> {
    require(size > 0) { "Chunk size must be positive, but was $size." }
    return sequence {
        val iterator = this@chunkedSequence.iterator()
        while (iterator.hasNext()) {
            val chunk = mutableListOf<T>()
            for (i in 1..size) {
                if (iterator.hasNext()) {
                    chunk.add(iterator.next())
                } else {
                    break
                }
            }
            if (chunk.isNotEmpty()) {
                yield(chunk)
            }
        }
    }
}