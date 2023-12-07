package net.barashev.dbi2023

import java.util.function.Function
import kotlin.math.ceil

/**
 * Mikhail Rodionychev
 */
class BlockNestedJoin(private val storageAccessManager: StorageAccessManager, private val cache: PageCache) :
    InnerJoin {
    override fun <T : Comparable<T>> join(leftTable: JoinOperand<T>, rightTable: JoinOperand<T>): JoinOutput {
        val leftPages = storageAccessManager.createFullScan(leftTable.tableName).pages().iterator()
        val rightPagesScan = storageAccessManager.createFullScan(rightTable.tableName)

        val outputTableUUID = java.util.UUID.randomUUID().toString()
        val outputTableOId = storageAccessManager.createTable(outputTableUUID)

        val builder = TableBuilder(storageAccessManager, cache, outputTableOId)

        leftPages.asSequence().chunked(cache.capacity - 2).forEach { chunk ->
            chunk.forEach { left ->
                rightPagesScan.pages().forEach { right ->
                    pageJoin(
                        left,
                        leftTable.joinAttribute,
                        right,
                        rightTable.joinAttribute,
                        builder
                    )
                }
            }
        }

        builder.close()

        return object : JoinOutput {
            val iterator = storageAccessManager.createFullScan(outputTableUUID).records { it }.iterator()
            override fun hasNext(): Boolean = iterator.hasNext()

            override fun next(): Pair<ByteArray, ByteArray> {
                val first = iterator.next()
                if (!hasNext()) {
                    throw RuntimeException("Records in the output of the join operation are not paired!")
                }
                val second = iterator.next()
                return Pair(first, second)
            }

            override fun close() {
                storageAccessManager.deleteTable(outputTableUUID)
            }
        }
    }

    private fun <T : Comparable<T>> pageJoin(
        leftPage: CachedPage,
        leftJoinAttribute: Function<ByteArray, T>,
        rightPage: CachedPage,
        rightJoinAttribute: Function<ByteArray, T>,
        builder: TableBuilder
    ) {
        leftPage.allRecords().values.map { leftJoinAttribute.apply(it.bytes) to it.bytes }.forEach { leftTuple ->
            rightPage.allRecords().values.map { rightJoinAttribute.apply(it.bytes) to it.bytes }
                .forEach { rightTuple ->
                    if (leftTuple.first == rightTuple.first) {
                        builder.insert(leftTuple.second)
                        builder.insert(rightTuple.second)
                    }
                }
        }
    }
}