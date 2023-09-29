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
 */

package net.barashev.dbi2023.fake

import net.barashev.dbi2023.*
import java.util.function.Function

/**
 * This is a fake merge sort that sorts the whole input in-memory.
 */
class FakeMergeSort(private val storageAccessManager: StorageAccessManager, private val cache: PageCache):
    MultiwayMergeSort {
    override fun <T : Comparable<T>> sort(tableName: String, comparableValue: Function<ByteArray, T>): String {
        val raw = storageAccessManager.createFullScan(tableName).records {
            it
        }.toList()
        val unsorted = raw.map { comparableValue.apply(it) }
        val sortedValues = unsorted.mapIndexed { index, t -> t to index }.sortedBy { it.first }
        val outOid = storageAccessManager.createTable("output")
        TableBuilder(storageAccessManager, cache, outOid).let {builder ->
            sortedValues.forEach { builder.insert(raw[it.second]) }
        }
        return "output"
    }
}

/**
 * Fake hash table that keeps all its records in memory.
 */
class FakeHashTable<T>(private val bucketTable: List<Pair<Bucket, List<ByteArray>>>, private val hashKey: Function<ByteArray, T>): Hashtable<T> {
    override val buckets: List<Bucket>
        get() = bucketTable.map { it.first }

    override fun <T> find(key: T): Iterable<ByteArray> {
        val hashCode = key.hashCode() % bucketTable.size
        return bucketTable[hashCode].second.filter { hashKey.apply(it) == key }
    }
}

/**
 * Fake hash table builder that creates hash tables in memory.
 */
class FakeHashTableBuilder(private val storageAccessManager: StorageAccessManager, private val cache: PageCache): HashtableBuilder {

    override fun <T> hash(tableName: String, bucketCount: Int, hashKey: Function<ByteArray, T>): Hashtable<T> {
        val bucketTable = mutableListOf<Pair<Bucket, MutableList<ByteArray>>>()
        repeat(bucketCount) {
            bucketTable.add(
                Bucket(it, "${tableName}_hash_${kotlin.random.Random.nextInt()}_${it}", 1)
                    to mutableListOf()
            )
        }
        storageAccessManager.createFullScan(tableName).pages().forEach {page ->
            page.allRecords().values.forEach {getRecord ->
                if (getRecord.isOk) {
                    val record = getRecord.bytes
                    val key = hashKey.apply(record)
                    val hashCode = key.hashCode() % bucketCount
                    bucketTable[hashCode].second.add(record)
                }
            }
        }
        return FakeHashTable(bucketTable, hashKey)
    }

}