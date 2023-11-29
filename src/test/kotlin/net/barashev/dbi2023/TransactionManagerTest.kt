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

import net.barashev.dbi2023.app.initializeFactories
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.concurrent.CountDownLatch

class TransactionManagerTest {
    private lateinit var storage: Storage
    @BeforeEach
    fun initialize() {
        storage = createHardDriveEmulatorStorage()
        initializeFactories(storage)
    }

    @Test
    fun `smoke test of two transactions with fake scheduler and wal`() {
        val scheduler = FakeScheduler()
        val wal = FakeWAL()
        val txnManager = TransactionManager(scheduler, LogManager(storage, 20, wal))

        val countDown = CountDownLatch(2)
        txnManager.txn {cache, txnId ->
            cache.get(10).use {
                it.putRecord(Record1(intField(42)).asBytes())
            }
            txnManager.commit(txnId)
            countDown.countDown()
        }
        txnManager.txn {cache, txnId ->
            cache.get(11).use {
                it.allRecords()
                it.putRecord(Record1(intField(11)).asBytes())
            }
            txnManager.abort(txnId)
            countDown.countDown()
        }

        countDown.await()
        println(wal)
    }
}