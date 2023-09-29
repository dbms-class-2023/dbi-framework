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
package net.barashev.dbi2023

import java.util.function.Function

/**
 * This operation sorts a table using order defined by Comparable objects parsed from the table records.
 * The operation returns a sorted copy of the input table contents as a new temporary table.
 */
interface MultiwayMergeSort : AutoCloseable {
    /**
     * Sorts a table with the name tableName by values extracted from the table records with comparableValue function.
     *
     * @returns the name of the output table. The output table is owned by the client, and it is client's responsibility
     *          to delete the output table when it is not needed anymore.
     */
    fun <T: Comparable<T>> sort(tableName: String, comparableValue: Function<ByteArray, T>): String

    /**
     * Releases all the temporary resources, such as intermediate tables, except for the output table.
     */
    override fun close() {}
}

/**
 * This builder object takes the input table and hashes its records using the hash key extracted by the provided function.
 * The builder shall close all resources that it might open when building the result.
 *
 * @param input table name
 * @param bucketCount how many buckets shall be in the result
 * @param hashKey function for extracting the hash key values from the input table records
 */
interface HashtableBuilder {
    /**
     * Hashes the input table records using bucketCount buckets and keys returned by hashKey function.
     * @return a list of created buckets metadata, in the ascending order of bucket numbers.
     */
    fun <T> hash(tableName: String, bucketCount: Int, hashKey: Function<ByteArray, T>): Hashtable<T>
}

/**
 * Hash table bucket with 0-based bucket number, name of a temporary table where bucket records are stored and
 * the total count of pages in the bucket.
 */
data class Bucket(val num: Int, val tableName: String, val pageCount: Int)

/**
 * A simple interface to already built hash table.
 */
interface Hashtable<T> {
    /** A list of buckets in this hash table */
    val buckets: List<Bucket>

    /**
     * Finds the records with the given value of the hash key. Returns an object that allows for iteration over such rows.
     * Returns empty list if there are no records by the given search key.
     */
    fun <T> find(key: T): Iterable<ByteArray>
}

object Operations {
    /**
     * Creates an instance of MultiwayMergeSort using the given storage access manager and page cache.
     */
    var sortFactory: (StorageAccessManager, PageCache) -> MultiwayMergeSort = { _, _  ->
        object : MultiwayMergeSort {
            override fun <T: Comparable<T>> sort(tableName: String, comparableValue: Function<ByteArray, T>): String {
                TODO("""Please DO NOT write your code here! Set the factory instance where you need it: Operations.sortFactory = """)
            }
        }
    }

    /**
     * Creates an instance of a hashtable builder using the given storage access manager and page cache.
     */
    var hashFactory: (StorageAccessManager, PageCache) -> HashtableBuilder = { _, _ ->
        object : HashtableBuilder {
            override fun <T> hash(tableName: String, bucketCount: Int, hashKey: Function<ByteArray, T>): Hashtable<T> {
                TODO("""Please DO NOT write your code here! Set the factory instance where you need it: Operations.hashFactory = """)
            }
        }
    }
}

