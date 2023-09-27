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
