/*
 * Copyright 2023 Dmitry Barashev, JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.barashev.dbi2023

import java.util.function.Function
import kotlin.jvm.Throws

enum class IndexMethod {
    BTREE, HASH
}

/**
 * Index structure interface.
 * Type argument T indicates a type of the attribute being indexed.
 *
 * This index maps index keys, that is, the values of the indexed attribute, to identifiers of pages where
 * a record with the matching value of the indexed attribute can be found.
 *
 * It is assumed that each index key has at most one matching PageId (which essentially means that it is a unique index)
 */
interface Index<T: Comparable<T>> {
    /**
     * Searches for the specified index key and returns an identifier of a table page where a record with such
     * value of the indexed attribute can be found.
     */
    fun lookup(indexKey: T): List<PageId>
}

class IndexException(message: String): Exception(message)

/**
 * Factory object for building and opening indexes.
 * We build/open indexes for a table with the specified tableName and write index pages as pages of a table
 * with indexTableName.
 *
 * Type arguments T and S indicate the JVM type of the indexed attribute and corresponding AttributeType class.
 */
interface IndexManager {
    /**
     * Builds index structure from the values produced with indexKey function from the records of tableName table.
     * Index pages shall be written as indexTableName pages to the storage.
     * keyType attribute instance may be used to serialize the index keys in the index records.
     *
     * Calling this method will create (and possible destroy previous) disk pages, so it shall be used with care.
     */
    fun <T: Comparable<T>, S: AttributeType<T>> build(
        tableName: String,
        indexTableName: String,
        method: IndexMethod = IndexMethod.BTREE,
        isUnique: Boolean,
        keyType: S,
        indexKey: Function<ByteArray, T>): Index<T>


    /**
     * Opens an index structure which was previously built from the values produced with indexKey function from the records of tableName table,
     * and written as indexTableName pages to the storage.
     * keyType attribute instance may be used to (de)serialize the index keys in the index records.
     *
     * This method per se is read-only, and won't modify the index pages.
     * @throws IndexException if there is no indexTableName table or if its pages are not a valid index of the specified index method.
     */
    @Throws(IndexException::class)
    fun <T: Comparable<T>, S: AttributeType<T>> open(
        tableName: String,
        indexTableName: String,
        method: IndexMethod = IndexMethod.BTREE,
        isUnique: Boolean,
        keyType: S, indexKey:
        Function<ByteArray, T>): Index<T>
}

typealias IndexFactory = (StorageAccessManager, PageCache) -> IndexManager

/**
 * Singleton object for accessing index factories
 */
object Indexes {
    /**
     * Index factory instance. The value can be set from the implementation/test code.
     */
    var indexFactory: IndexFactory = { _, _ -> TODO("Please DO NOT WRITE your code here. Set the factory instance where you need it: Indexes.indexFactory = ") }
}
