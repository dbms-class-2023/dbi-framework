package net.barashev.dbi2023

import java.util.function.Function
import kotlin.math.pow
import kotlin.random.Random

class FastSortImpl(private val storageAccessManager: StorageAccessManager, private val cache: PageCache): MultiwayMergeSort {
    override fun <T : Comparable<T>> sort(tableName: String, comparableValue: Function<ByteArray, T>): String {
        val random = Random.nextLong()
        val sortedRuns = mutableListOf<String>()

        val chunkSize = cache.capacity/2
        val chunkedPages = storageAccessManager.createFullScan(tableName).pages().asSequence().chunked(chunkSize)
        chunkedPages.forEachIndexed { idx, it ->
            val chunk = it.map { page -> cache.getAndPin(page.id) }
            // We pretend that this is an in-place sort that modifies the cached page bytes
            val recordList = mutableListOf<Pair<T, ByteArray>>()
            chunk.forEach { page ->
                page.allRecords().forEach { record ->
                    assert(record.value.isOk)
                    recordList.add(comparableValue.apply(record.value.bytes) to record.value.bytes)
                }
            }
            recordList.sortBy { it.first }

            val outTableName = "$tableName-sorted-$random-$idx"
            val outOid = storageAccessManager.createTable(outTableName)
            sortedRuns.add(outTableName)
            TableBuilder(storageAccessManager, cache, outOid).use {
                recordList.forEach { record ->
                    it.insert(record.second)
                }
            }
            chunk.forEach { page -> page.close() }
        }
        if (sortedRuns.size > chunkSize) {
            error("Unfortunately I can't sort a table that exceeds the limit of ${chunkSize.toDouble().pow(2)} pages")
        }

        val sortedRunIterators = sortedRuns.map {sortedRunTable ->
            BufferedRecordIterator(storageAccessManager, cache, 1, comparableValue, sortedRunTable)
        }.toList()

        val outTableName = "$tableName-sorted-$random"
        val outOid = storageAccessManager.createTable(outTableName)
        TableBuilder(storageAccessManager, cache, outOid).use {out ->
            while (true) {
                var minIdx = -1
                var minRecord: Pair<T, ByteArray>? = null
                sortedRunIterators.forEachIndexed { idx, it ->
                    if (it.hasNext()) {
                        if (minRecord == null || it.topRecord().first < minRecord.first) {
                            minRecord = it.topRecord()
                            minIdx = idx
                        }
                    }
                }
                if (minRecord == null) {
                    break
                }
                out.insert(minRecord.second)
                sortedRunIterators[minIdx].pull()
            }
        }
        sortedRunIterators.forEach { it.close() }
        return outTableName
    }

}