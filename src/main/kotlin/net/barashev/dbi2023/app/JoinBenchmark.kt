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
                println("Failed to join:")
                println(it.message)
            },
            onSuccess = {
                val cost0 = storage.totalAccessCost
                it.join(
                    JoinOperand("flight") {flightRecord(it).value1 },
                    JoinOperand("ticket") { ticketRecord(it).value1 }
                ).apply {
                    println("""The results of flight JOIN ticket:
                        |-------------------------------------------
                    """.trimMargin())
                }.forEach {
                    println("Flight: ${flightRecord(it.first)}")
                    println("Ticket: ${ticketRecord(it.second)}")
                    println("-------------------")
                }
                println("Join cost: ${storage.totalAccessCost - cost0}")
                println("Cache stats: ${cache.stats}")
            }
        )
    }


}