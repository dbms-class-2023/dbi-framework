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

import net.barashev.dbi2023.*
import net.datafaker.Faker
import kotlin.random.Random

typealias MyTableBuilder = TableBuilder
class DataGenerator(
    val storageAccessManager: StorageAccessManager,
    val cache: PageCache,
    scale: Int = 1,
    fixedRowCount: Boolean,
    disableStatistics: Boolean
) : AutoCloseable {
    private val planetCount = (if (fixedRowCount) 100 else Random.nextInt(10, 100)) * scale/5
    private val spacecraftCount = (if (fixedRowCount) 20 else Random.nextInt(10, 20)) * scale/5
    val flightCount = (if (fixedRowCount) 200 else Random.nextInt(500, 1000)) * scale
    private val ticketCount = flightCount * 3
    val faker = Faker()
    private val planetTableOid = storageAccessManager.createTable("planet")
    val planetBuilder = MyTableBuilder(storageAccessManager, cache, planetTableOid)

    private val spacecraftTableOid = storageAccessManager.createTable("spacecraft")
    val spacecraftBuilder = MyTableBuilder(storageAccessManager, cache, spacecraftTableOid)

    private val flightTableOid = storageAccessManager.createTable("flight")
    val flightBuilder = MyTableBuilder(storageAccessManager, cache, flightTableOid)

    private val ticketTableOid = storageAccessManager.createTable("ticket")
    val ticketBuilder = MyTableBuilder(storageAccessManager, cache, ticketTableOid)

    init {
        println("--------------------")
        println("Inserting $planetCount planets, $spacecraftCount spacecrafts, $flightCount flights and $ticketCount tickets")
        println("--------------------")
        populatePlanets(planetCount)
        populateSpacecrafts(spacecraftCount)
        populateFlights(flightCount, planetCount, spacecraftCount)
        populateTickets(ticketCount, flightCount)
        println("--------------------")
        println("Done!")
        println("--------------------")

        if (!disableStatistics) {
            println("--------------------")
            println("Collecting statistics")
            generateStatistics()
            println("Done!")
            println("--------------------")
        }

        Thread.sleep(2000)
    }

    override fun close() {
        planetBuilder.close()
        spacecraftBuilder.close()
        flightBuilder.close()
        ticketBuilder.close()
    }
}

private fun DataGenerator.populatePlanets(planetCount: Int) {
    (1..planetCount).forEach {idx ->
        PlanetRecord(intField(idx), stringField(faker.starCraft().planet()), doubleField(Random.nextDouble(100.0, 900.0))).also {
            planetBuilder.insert(it.asBytes())
        }
    }
}

private fun DataGenerator.populateSpacecrafts(spacecraftCount: Int) {
    (1..spacecraftCount).forEach { idx ->
        SpacecraftRecord(intField(idx), stringField(faker.pokemon().name()), intField(Random.nextInt(1, 10))).also {
            spacecraftBuilder.insert(it.asBytes())
        }
    }
}

fun DataGenerator.insertFlight(flightId: Int, planetId: Int, spacecraftId: Int) {
    FlightRecord(intField(flightId), intField(planetId), intField(spacecraftId)).also {
        flightBuilder.insert(it.asBytes())
    }
}

private fun DataGenerator.populateFlights(flightCount: Int, planetCount: Int, spacecraftCount: Int) {
    (1..flightCount).forEach {idx ->
        insertFlight(idx, Random.nextInt(1, planetCount), Random.nextInt(1, spacecraftCount))
    }
}

private fun DataGenerator.populateTickets(ticketCount: Int, flightCount: Int) {
    (1..ticketCount).forEach {
        insertTicket(Random.nextInt(flightCount), faker.name().fullName(), Random.nextDouble(50.0, 200.0))
    }
}

fun DataGenerator.insertTicket(flightNum: Int, fullName: String, ticketPrice: Double) {
    TicketRecord(intField(flightNum), stringField(fullName), doubleField(ticketPrice)).also {
        ticketBuilder.insert(it.asBytes())
    }

}

private fun DataGenerator.generateStatistics() {
    Statistics.managerFactory(storageAccessManager, cache).apply {
        buildAttributeStatistics(
            "planet",
            "id",
            intField().first,
            10
        ) { planetRecord(it).component1() }
        buildAttributeStatistics(
            "planet",
            "name",
            stringField().first,
            10
        ) { planetRecord(it).component2() }
        buildAttributeStatistics(
            "planet",
            "distance",
            doubleField().first,
            10
        ) { planetRecord(it).component3() }
        buildAttributeStatistics(
            "spacecraft",
            "id",
            intField().first,
            10
        ) { spacecraftRecord(it).component1() }
        buildAttributeStatistics(
            "spacecraft",
            "name",
            stringField().first,
            10
        ) { spacecraftRecord(it).component2() }
        buildAttributeStatistics(
            "spacecraft",
            "capacity",
            intField().first,
            10
        ) { spacecraftRecord(it).component3() }
        buildAttributeStatistics(
            "flight",
            "num",
            intField().first,
            10
        ) { flightRecord(it).component1() }
        buildAttributeStatistics(
            "flight",
            "planet_id",
            intField().first,
            10
        ) { flightRecord(it).component2() }
        buildAttributeStatistics(
            "flight",
            "spacecraft_id",
            intField().first,
            10
        ) { flightRecord(it).component3() }
        buildAttributeStatistics(
            "ticket",
            "flight_num",
            intField().first,
            10
        ) { ticketRecord(it).component1() }
        buildAttributeStatistics(
            "ticket",
            "pax_name",
            stringField().first,
            10
        ) { ticketRecord(it).component2() }
        buildAttributeStatistics(
            "ticket",
            "price",
            doubleField().first,
            10
        ) { ticketRecord(it).component3() }

        buildTableStatistics("planet", ::planetRecord)
        buildTableStatistics("spacecraft", ::spacecraftRecord)
        buildTableStatistics("flight", ::flightRecord)
        buildTableStatistics("ticket", ::ticketRecord)
    }
}
