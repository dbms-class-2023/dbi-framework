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
 *
 *
 */

package net.barashev.dbi2023

import java.util.function.Function

class SimpleStatisticsManager(private val storageAccess: StorageAccessManager): StatisticsManager {

    private val attributeStatistics = mutableMapOf<String, AttributeStatistics<*>>()
    private val tableStatistics = mutableMapOf<String, TableStatistics>()
    override fun <T : Comparable<T>> buildAttributeStatistics(
        tableName: String,
        attributeName: String,
        attributeType: AttributeType<T>,
        bucketCount: Int,
        attributeValue: AttributeValueParser<T>
    ): AttributeStatistics<T> {

        val attributeFullName = "$tableName.$attributeName"
        val valueCounts = mutableMapOf<T, Int>()
        var totalValueCount = 0
        storageAccess.createFullScan(tableName).records(attributeValue).forEach {
            val key = it
            valueCounts.put(key, valueCounts.getOrDefault(key, 0) + 1)
            totalValueCount++
        }

        val bucketHeight = totalValueCount/bucketCount
        val buckets = mutableListOf<HistogramBucket<T>>()
        valueCounts.let {
            var currentHeight = 0
            var minKey: T? = null
            it.forEach { key, count ->
                if (currentHeight == 0) {
                    minKey = key
                }
                currentHeight += count
                if (currentHeight >= bucketHeight) {
                    buckets.add(HistogramBucket(minKey!!, key, currentHeight))
                    currentHeight = 0
                }
            }
        }
        return AttributeStatistics(attributeFullName, valueCounts.size, buckets).also {
            attributeStatistics[attributeFullName] = it
        }
    }

    override fun getAttributeStatistics(
        tableName: String,
        attributeName: String,
    ): AttributeStatistics<*>? {
        return attributeStatistics["$tableName.$attributeName"]
    }

    override fun <T> buildTableStatistics(tableName: String, recordParser: Function<ByteArray, T>): TableStatistics =
        TableStatistics(storageAccess.createFullScan(tableName).records(recordParser).count()).also {
            tableStatistics[tableName] = it
        }


    override fun getTableStatistics(tableName: String): TableStatistics? = tableStatistics[tableName]
}