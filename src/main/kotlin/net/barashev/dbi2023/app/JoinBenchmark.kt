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

package net.barashev.dbi2023.app

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.enum
import com.github.ajalt.clikt.parameters.types.int
import net.barashev.dbi2023.JoinAlgorithm
import net.barashev.dbi2023.JoinOperand
import net.barashev.dbi2023.Operations
import net.barashev.dbi2023.createHardDriveEmulatorStorage
import org.slf4j.LoggerFactory

class JoinBenchmark: CliktCommand() {
    val dataScale: Int by option(help="Test data scale [default=1]").int().default(1)
    val cacheSize: Int by option(help="Page cache size [default=100]").int().default(System.getProperty("cache.size", "100").toInt())
    val cacheImpl: String by option(help="Cache implementation [default=aging]").default(System.getProperty("cache.impl", "aging"));
    val realHash by option(help="Use the real hash implementation [default=true]").flag(default = true)
    val realSort by option(help="Use the real merge-sort implementation [default=false]").flag(default = false)
    val joinAlgorithm by option(help="Join algorithm to use [default=NESTED_LOOPS]").enum<JoinAlgorithm>().default(JoinAlgorithm.NESTED_LOOPS)
    override fun run() {
        val storage = createHardDriveEmulatorStorage()
        val (cache, accessManager) = initializeFactories(storage = storage, cacheSize = cacheSize,
            cacheImpl = cacheImpl,
            hashImpl = if (realHash) "real" else "fake",
            sortImpl = if (realSort) "real" else "fake"
        )
        DataGenerator(accessManager, cache, dataScale, fixedRowCount = true, disableStatistics = true).use{}
        Operations.innerJoinFactory(accessManager, cache, joinAlgorithm).fold(
            onFailure = {
                LOGGER.error("Failed to join: ${it.message}", it)
            },
            onSuccess = {
                val cost0 = storage.totalAccessCost
                cache.flush()
                val leftOperand = JoinOperand("flight") { flightRecord(it).value1 }
                val rightOperand = JoinOperand("ticket") { ticketRecord(it).value1 }

                val actualResult = mutableSetOf<Pair<TicketRecord, FlightRecord>>()
                it.join(leftOperand, rightOperand).apply {
                    LOGGER.debug("""The results of flight JOIN ticket:
                        |-------------------------------------------
                    """.trimMargin())
                }.forEach {
                    actualResult.add(ticketRecord(it.second) to flightRecord(it.first))
                    LOGGER.debug("Flight: ${flightRecord(it.first)}")
                    LOGGER.debug("Ticket: ${ticketRecord(it.second)}")
                    LOGGER.debug("-------------------")
                }
                println("JOIN COST:${storage.totalAccessCost - cost0}")
                LOGGER.debug("Cache stats: ${cache.stats}")

                val flightMap = accessManager.createFullScan("flight").records {
                    leftOperand.joinAttribute.apply(it) to flightRecord(it)
                }.toMap()
                val expectedResult = accessManager.createFullScan("ticket").records {
                    rightOperand.joinAttribute.apply(it) to ticketRecord(it)
                }.map { ticket ->
                    val flight = flightMap[ticket.first] ?: error("Can't find a flight by the flight number ${ticket.first}")
                    ticket.second to flight
                }.toSet()

                if (expectedResult != actualResult) {
                    val expectedString = expectedResult.map{"${it.first}: ${it.second}" }.joinToString(separator = "\n", postfix = "\n")
                    val actualString = actualResult.map{"${it.first}: ${it.second}" }.joinToString(separator = "\n", postfix = "\n")
                    if (expectedResult.size != actualResult.size) {
                        LOGGER.error("""|
                        |The result of the merge is not the one we expected. Expected ${expectedResult.size} pairs, actual ${actualResult.size} pairs
                        |
                        """.trimMargin())
                    } else {
                        LOGGER.error("The result size is as expected, but the contents is not")
                        LOGGER.error("""-------------------------------
                            EXPECTED:
                            $expectedString""")
                        LOGGER.error("""-------------------------------
                            ACTUAL:
                            $actualString""")
                    }
                    System.exit(1)
                }

            }
        )
    }
}

private val LOGGER = LoggerFactory.getLogger("Join.Benchmark")