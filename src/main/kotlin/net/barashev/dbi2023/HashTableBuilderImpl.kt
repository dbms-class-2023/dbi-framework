package net.barashev.dbi2023

import java.util.function.Function

/**
 * Daniil Karol
 */
class HashTableBuilderImpl(private val storageAccessManager: StorageAccessManager, private val cache: PageCache) :
    HashtableBuilder {
    override fun <T> hash(tableName: String, bucketCount: Int, hashKey: Function<ByteArray, T>): Hashtable<T> {
        val bucketTable = mutableListOf<TableBuilder>()
        val numsAndTableNames = mutableListOf<Pair<Int, String>>()
        repeat(bucketCount) {
            val tableHashName = "${tableName}_hash_${it}"
            val oid = storageAccessManager.createTable(tableHashName)
            numsAndTableNames.add(it to tableHashName)
            bucketTable.add(TableBuilder(storageAccessManager, cache, oid))
        }
        storageAccessManager.createFullScan(tableName).pages().forEach { page ->
            page.allRecords().values.forEach { getRecord ->
                if (getRecord.isOk) {
                    val record = getRecord.bytes
                    val key = hashKey.apply(record)
                    val hashCode = key.hashCode().absMod(bucketCount)
                    bucketTable[hashCode].insert(record)
                }
            }
        }
        bucketTable.forEach(TableBuilder::close)
        return HashTableImpl(
            storageAccessManager,
            numsAndTableNames.map { Bucket(it.first, it.second, storageAccessManager.pageCount(it.second)) },
            hashKey
        )
    }

    private fun Int.absMod(divisor: Int): Int {
        val result = this % divisor
        return if (result < 0) result + divisor else result
    }

}

class HashTableImpl<T>(
    private val storageAccessManager: StorageAccessManager,
    override val buckets: List<Bucket>,
    private val hashKey: Function<ByteArray, T>
) : Hashtable<T> {

    override fun <T> find(key: T): Iterable<ByteArray> {
        val hashCode = key.hashCode() % buckets.size
        val bucket = buckets[hashCode]
        val records = storageAccessManager.createFullScan(bucket.tableName).records { it }
        return records.filter { hashKey.apply(it) == key }
    }
}