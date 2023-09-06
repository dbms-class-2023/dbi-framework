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

import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.*
import kotlin.test.assertEquals

class RecordsTest {
    @Test
    fun `fixed size record write-read test`() {
        val rec = Record2(intField(42), intField(146))
        val bytes = rec.asBytes()
        assertEquals(8, bytes.size)

        Record2(intField(), intField()).fromBytes(bytes).let {
            assertEquals(42, it.value1)
            assertEquals(146, it.value2)
        }
    }

    @Test
    fun `variable size record write-read test`() {
        val rec = Record2(stringField("lorem ipsum"), stringField("dolor sit amet"))
        val bytes = rec.asBytes()
        assertEquals("lorem ipsum".length*2 + "dolor sit amet".length*2 + Int.SIZE_BYTES*2, bytes.size)

        Record2(stringField(), stringField()).fromBytes(bytes).let {
            assertEquals("lorem ipsum", it.value1)
            assertEquals("dolor sit amet", it.value2)
        }
    }

    @Test
    fun `mixed size field record`() {
        val rec = Record3(stringField("Привет Мир!"), intField(42), stringField("Hello World!"))
        val bytes = rec.asBytes()
        Record3(stringField(), intField(), stringField()).fromBytes(bytes).let {
            assertEquals("Привет Мир!", it.value1)
            assertEquals(42, it.value2)
            assertEquals("Hello World!", it.value3)
        }
    }

    @Test
    fun `record destructuring`() {
        val (c1, c2, c3) = Record3(intField(42), stringField("The question"), dateField(Date.from(Instant.EPOCH)))
        assertEquals(42, c1)
        assertEquals("The question", c2)
        assertEquals(Date.from(Instant.EPOCH), c3)
    }

}