package net.barashev.dbi2023

import net.barashev.dbi2023.fake.FakeNestedLoops
import kotlin.math.roundToInt

/**
 * Mikhail Rodionychev
 */
class HashJoinImpl(private val storageAccessManager: StorageAccessManager, val cache: PageCache) : InnerJoin {
    private val maxCacheSize = (cache.capacity*0.8).roundToInt()
    override fun <T : Comparable<T>> join(leftTable: JoinOperand<T>, rightTable: JoinOperand<T>): JoinOutput {
        val hash = Operations.hashFactory(storageAccessManager, cache)
        val nlj = Operations.innerJoinFactory(storageAccessManager, cache, JoinAlgorithm.NESTED_LOOPS)
            .getOrElse { FakeNestedLoops(storageAccessManager) }

        if (storageAccessManager.pageCount(leftTable.tableName) <= maxCacheSize) {
            return nlj.join(leftTable, rightTable)
        }

        val leftHash = hash.hash(leftTable.tableName, maxCacheSize, leftTable.joinAttribute)
        val rightHash = hash.hash(rightTable.tableName, maxCacheSize, rightTable.joinAttribute)
        val joinResults = leftHash.buckets.zip(rightHash.buckets)
            .map {
                nlj.join(
                    JoinOperand(
                        it.first.tableName, leftTable.joinAttribute
                    ),
                    JoinOperand(
                        it.second.tableName, rightTable.joinAttribute
                    )
                )
            }

        return object : JoinOutput {
            val iterator = joinResults.fold(emptySequence<Pair<ByteArray, ByteArray>>()) { acc, joinResult -> acc + joinResult.asSequence()}.iterator()
            override fun hasNext(): Boolean = iterator.hasNext()

            override fun next(): Pair<ByteArray, ByteArray> = iterator.next()

            override fun close() {
                joinResults.forEach {
                    it.close()
                }
            }
        }
    }
}