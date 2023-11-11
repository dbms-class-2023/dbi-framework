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

/**
 * A single bucket of an attribute value histogram.
 * A bucket indicates how many values are in a semi-open range [rangeStart, rangeEnd)
 */
data class HistogramBucket<T>(val rangeBegin: T, val rangeEnd: T, val valueCount: Int)

/**
 * A single attribute statistics which includes:
 * - attribute cardinality (total number of distinct attribute values)
 * - an equi-height histogram of the attribute values distribution
 */
data class AttributeStatistics<T: Comparable<T>>(
    val attributeName: String,
    val cardinality: Int,
    val histogramBuckets: List<HistogramBucket<T>>
)

interface StatisticsManager {
    /**
     * Creates statistics for the given attribute of a table, using specified bucketCount number of histogram buckets.
     * Statistic data shall be persistent, and shall be returned from getStatistics calls without re-calculating
     */
    fun <T: Comparable<T>> buildStatistics(
        tableName: String, attributeName: String, attributeType: AttributeType<T>, bucketCount: Int, attributeValue: Function<ByteArray, T>): AttributeStatistics<T>

    /**
     * @return Returns statistics previously build for the given attribute of a table, or null if no statistics records
     * were found for that attribute.
     */
    fun <T: Comparable<T>> getStatistics(tableName: String, attributeName: String, attributeType: AttributeType<T>): AttributeStatistics<T>?
}

object Statistics {
    /**
     * Factory method for creating StatisticsManager instances.
     * Please set your own factory instance from your own code.
     */
    var managerFactory: (StorageAccessManager, PageCache) -> StatisticsManager = { _, _ -> FakeStatisticsManager() }
}


class FakeStatisticsManager: StatisticsManager {
    override fun <T : Comparable<T>> buildStatistics(
        tableName: String,
        attributeName: String,
        attributeType: AttributeType<T>,
        bucketCount: Int,
        attributeValue: Function<ByteArray, T>
    ): AttributeStatistics<T> = AttributeStatistics(attributeName, 0, emptyList())

    override fun <T : Comparable<T>> getStatistics(
        tableName: String,
        attributeName: String,
        attributeType: AttributeType<T>
    ): AttributeStatistics<T> = AttributeStatistics(attributeName, 0, emptyList())
}

