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

import net.barashev.dbi2023.app.*
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class QueryExecutorTest {
    private fun generateData(storageAccessManager: StorageAccessManager, cache: PageCache) {
        DataGenerator(storageAccessManager, cache, 1, true, true).use {
            it.insertFlight(it.flightCount + 1, 1, 2)
        }
    }

    @Test
    fun `push down predicate to join leaf`() {
        val storage = createHardDriveEmulatorStorage()
        val (cache, storageAccessManager) = initializeFactories(storage, storage,20)
        generateData(storageAccessManager, cache)

        val plan = QueryPlan(listOf(
            JoinNode(
                JoinSpec("planet", "id").also {
                    it.filterBy(FilterSpec("planet", "id", 1 as Comparable<Any>, EQ))
                },
                JoinSpec("flight", "planet_id").also {
                    it.filterBy(FilterSpec("flight", "num", 2 as Comparable<Any>, GT))
                }
            ),
            JoinNode(
                JoinSpec("flight", "spacecraft_id"),
                JoinSpec("spacecraft", "id").also {
                    it.filterBy(FilterSpec("spacecraft", "id", 2 as Comparable<Any>, EQ))
                }
            )
        ), emptyList())

        var resultRowCount = 0
        val resultSpec = QueryExecutor(storageAccessManager, cache, tableRecordParsers, attributeValueParsers).execute(plan)
        storageAccessManager.createFullScan(resultSpec.tableName).records { bytes ->
            parseJoinedRecord(bytes, resultSpec.realTables, tableRecordParsers)
        }.forEach {
            resultRowCount++
            it.entries.forEach { (tableName, recordBytes) ->
                when (tableName) {
                    "planet" -> assertEquals<Any>(1, attributeValueParsers["planet.id"]!!.apply(recordBytes))
                    "flight" -> assertTrue(attributeValueParsers["flight.num"]!!.apply(recordBytes) > 2)
                    "spacecraft" -> assertEquals<Any>(2, attributeValueParsers["spacecraft.id"]!!.apply(recordBytes))
                }
            }
        }
        assertTrue(resultRowCount > 0)
    }

    @Test
    fun `push down predicate to join inner node`() {
        val storage = createHardDriveEmulatorStorage()
        val (cache, storageAccessManager) = initializeFactories(storage, storage,20)
        generateData(storageAccessManager, cache)

        val plan = QueryPlan(listOf(
            JoinNode(
                JoinSpec("planet", "id").also {
                    it.filterBy(FilterSpec("planet", "id", 1 as Comparable<Any>, EQ))
                },
                JoinSpec("flight", "planet_id")
            ),
            JoinNode(
                JoinSpec("flight", "spacecraft_id").also {
                    it.filterBy(FilterSpec("flight", "num", 100 as Comparable<Any>, LE))
                },
                JoinSpec("spacecraft", "id")
            )
        ), emptyList())

        var resultRowCount = 0
        val resultSpec = QueryExecutor(storageAccessManager, cache, tableRecordParsers, attributeValueParsers).execute(plan)
        storageAccessManager.createFullScan(resultSpec.tableName).records { bytes ->
            parseJoinedRecord(bytes, resultSpec.realTables, tableRecordParsers)
        }.forEach {
            resultRowCount++
            it.entries.forEach { (tableName, recordBytes) ->
                when (tableName) {
                    "planet" -> assertEquals<Any>(1, attributeValueParsers["planet.id"]!!.apply(recordBytes))
                    "flight" -> assertTrue(attributeValueParsers["flight.num"]!!.apply(recordBytes) <= 100)
                }
            }
        }
        assertTrue(resultRowCount > 0)
    }

    @Test
    fun `apply filters after joins`() {
        val storage = createHardDriveEmulatorStorage()
        val (cache, storageAccessManager) = initializeFactories(storage, storage,20)
        generateData(storageAccessManager, cache)

        val plan = QueryPlan(listOf(
            JoinNode(JoinSpec("planet", "id"), JoinSpec("flight", "planet_id")),
            JoinNode(JoinSpec("flight", "spacecraft_id"), JoinSpec("spacecraft", "id"))
        ), listOf(
            FilterSpec("planet", "id", 1 as Comparable<Any>, EQ),
            FilterSpec("flight", "num", 2 as Comparable<Any>, GT),
            FilterSpec("spacecraft", "id", 2 as Comparable<Any>, EQ)
        ))

        var resultRowCount = 0
        val resultSpec = QueryExecutor(storageAccessManager, cache, tableRecordParsers, attributeValueParsers).execute(plan)
        storageAccessManager.createFullScan(resultSpec.tableName).records { bytes ->
            parseJoinedRecord(bytes, resultSpec.realTables, tableRecordParsers)
        }.forEach {
            resultRowCount++
            it.entries.forEach { (tableName, recordBytes) ->
                when (tableName) {
                    "planet" -> assertEquals<Any>(1, attributeValueParsers["planet.id"]!!.apply(recordBytes))
                    "flight" -> assertTrue(attributeValueParsers["flight.num"]!!.apply(recordBytes) > 2)
                    "spacecraft" -> assertEquals<Any>(2, attributeValueParsers["spacecraft.id"]!!.apply(recordBytes))
                }
            }
        }
        assertTrue(resultRowCount > 0)
    }

    @Test
    fun `filter using index`() {
        val storage = createHardDriveEmulatorStorage()
        val (cache, storageAccessManager) = initializeFactories(storage, storage,20)
        generateData(storageAccessManager, cache)
        storageAccessManager.createIndex("flight", "num", IntAttribute()) {
            flightRecord(it).value1
        }

        val plan = QueryPlan(listOf(
            JoinNode(
                JoinSpec("planet", "id"),
                JoinSpec("flight", "planet_id").also {
                    it.filterBy(FilterSpec("flight", "num", 2 as Comparable<Any>, EQ, accessMethod = TableAccessMethod.INDEX_SCAN))
                }
            ),
            JoinNode(
                JoinSpec("flight", "spacecraft_id"), JoinSpec("spacecraft", "id")
            )
        ), emptyList())

        var resultRowCount = 0
        val resultSpec = QueryExecutor(storageAccessManager, cache, tableRecordParsers, attributeValueParsers).execute(plan)
        storageAccessManager.createFullScan(resultSpec.tableName).records { bytes ->
            parseJoinedRecord(bytes, resultSpec.realTables, tableRecordParsers)
        }.forEach {
            resultRowCount++
            it.entries.forEach { (tableName, recordBytes) ->
                when (tableName) {
                    "flight" -> assertEquals<Any>(2, attributeValueParsers["flight.num"]!!.apply(recordBytes))
                }
            }
        }
        assertTrue(resultRowCount > 0)
    }

    @Test
    fun `temporary tables are deleted`() {
        val storage = createHardDriveEmulatorStorage()
        val (cache, storageAccessManager) = initializeFactories(storage, storage,20)
        generateData(storageAccessManager, cache)

        val plan = QueryPlan(parseJoinClause("planet.id:flight.planet_id flight.spacecraft_id:spacecraft.id"), emptyList())

        QueryExecutor(storageAccessManager, cache, tableRecordParsers, attributeValueParsers).execute(plan)
        assertFalse(storageAccessManager.tableExists("planet,flight"))
        assertTrue(storageAccessManager.tableExists("planet,flight,spacecraft"))
    }

}