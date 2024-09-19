package net.barashev.dbi2023

import java.util.function.Function
import kotlin.random.Random

class SlowSortImpl(private val storageAccessManager: StorageAccessManager, private val cache: PageCache): MultiwayMergeSort {
    private val closeables = mutableListOf<AutoCloseable>()

    override fun <T : Comparable<T>> sort(tableName: String, comparableValue: Function<ByteArray, T>): String {
        val outTable = "output${Random.nextLong()}"
        val outOid = storageAccessManager.createTable(outTable)
        TableBuilder(storageAccessManager, cache, outOid).use { outputBuilder ->
            var minRecord = storageAccessManager.createFullScan(tableName).records {
                comparableValue.apply(it) to it
            }.minByOrNull { it.first }

            // In this loop we find all the records with the minimum value and find the next minimum value.
            while (minRecord != null) {
                var nextMinRecord = minRecord!!
                storageAccessManager.createFullScan(tableName).records {
                    comparableValue.apply(it) to it
                }.forEach {
                    if (it.first == minRecord.first) {
                        outputBuilder.insert(it.second)
                        return@forEach
                    }

                    if (it.first < minRecord.first) {
                        // We already inserted it to the output on the previous scan.
                        return@forEach
                    }

                    if (nextMinRecord.first == minRecord.first) {
                        // It is the first record that is greater than the current minimum. Lets initialize
                        // nextMinRecord.
                        nextMinRecord = it
                    } else {
                        // It is NOT the first record that is greater than the current minimum. Maybe it is the
                        // next minumum?
                        if (it.first < nextMinRecord.first) {
                            nextMinRecord = it
                        }
                    }
                }
                if (nextMinRecord.first == minRecord.first) {
                    // We have not found any greater value
                    break
                }
                minRecord = nextMinRecord
            }
            closeables.add(AutoCloseable {
                storageAccessManager.deleteTable(outTable)
            })
            return outTable
        }
    }

    override fun close() {
        closeables.forEach { it.close() }
    }
}