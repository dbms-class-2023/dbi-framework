package net.barashev.dbi2023

/**
 * Andrei Pronichev
 */
class HashJoin(
    private val storageAccessManager: StorageAccessManager,
    private val cache: PageCache
) : InnerJoin {

    val intermediateResults = mutableListOf<JoinOutput>()

    private fun <T> getBuckets(table: JoinOperand<T>): List<Bucket> =
        Operations.hashFactory(storageAccessManager, cache).hash(
            table.tableName,
            (cache.capacity*0.8).toInt(),
            table.joinAttribute
        ).buckets

    override fun <T : Comparable<T>> join(
        leftTable: JoinOperand<T>,
        rightTable: JoinOperand<T>
    ): JoinOutput {
        // run block nlj if left page fits into cache
        if (storageAccessManager.pageCount(leftTable.tableName) <= (cache.capacity*0.8).toInt()) {
            println("fall back to nested loops because the size of ${leftTable.tableName} is ${storageAccessManager.pageCount(leftTable.tableName)} and it is less than the cache size")
            return nestedLoopJoin(leftTable, rightTable)
        }
        val leftBuckets = getBuckets(leftTable)
        val rightBuckets = getBuckets(rightTable)
        for (i in 0 until(leftBuckets.size)) {
            intermediateResults.add(nestedLoopJoin(
                JoinOperand(leftBuckets[i].tableName, leftTable.joinAttribute),
                JoinOperand(rightBuckets[i].tableName, rightTable.joinAttribute)
            ))

        }
        leftBuckets.forEach {storageAccessManager.deleteTable(it.tableName)}
        rightBuckets.forEach {storageAccessManager.deleteTable(it.tableName)}

        return object : JoinOutput {
            val iterator = intermediateResults.map { it.iterator().asSequence() }.asSequence().flatten().iterator()

            override fun hasNext(): Boolean = iterator.hasNext()

            override fun next(): Pair<ByteArray, ByteArray> = iterator.next()

            override fun close() {
                this@HashJoin.close()
            }

        }
    }

    override fun close() {
        intermediateResults.forEach(JoinOutput::close)
    }

    private fun <T : Comparable<T>> nestedLoopJoin(
        left: JoinOperand<T>,
        right: JoinOperand<T>
    ): JoinOutput =
        Operations.innerJoinFactory(storageAccessManager, cache, JoinAlgorithm.NESTED_LOOPS)
            .getOrThrow().join(left, right)

}