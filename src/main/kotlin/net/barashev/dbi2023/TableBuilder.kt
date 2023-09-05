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

/**
 * This class hides the complex details of building the table, such as adding new pages,
 * by providing one simple insert method.
 */
class TableBuilder(private val storageAccessManager: StorageAccessManager, private val cache: PageCache, private val tableOid: Oid): AutoCloseable {

    /**
     * Inserts a new record into the table. Adds new table pages if necessary.
     */
    fun insert(record: ByteArray) {
        // TODO: write your code here
    }

    /**
     * Closes the builder and releases resources if necessary.
     */
    override fun close() {
        // TODO: write your code here
    }
}
