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

// --------------------------------------------------------------------------------------------------------------------
// A collection of types and utilities for data serialization purposes.
// --------------------------------------------------------------------------------------------------------------------

import java.nio.ByteBuffer
import java.time.Instant
import java.util.*
import java.util.function.Function

/**
 * Attribute type instances can serialize and deserialize values of corresponding types to and from byte arrays.
 */
sealed class AttributeType<T>(val byteSize: Int = -1) {
    val isFixedSize: Boolean = byteSize > 0

    abstract fun asBytes(value: T): ByteArray

    abstract fun fromBytes(bytes: ByteArray): Pair<T, Int>

    abstract fun defaultValue(): T
}

class IntAttribute: AttributeType<Int>(Int.SIZE_BYTES) {
    override fun asBytes(value: Int): ByteArray = ByteBuffer.allocate(byteSize).also {
        it.putInt(value)
    }.array()

    override fun fromBytes(bytes: ByteArray) = ByteBuffer.wrap(bytes).int to Int.SIZE_BYTES
    override fun defaultValue(): Int  = 0

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        return true
    }

    override fun hashCode(): Int {
        return javaClass.hashCode()
    }

    override fun toString(): String {
        return "Int"
    }
}
fun intField(value: Int = 0) = IntAttribute() to value

class LongAttribute: AttributeType<Long>(Long.SIZE_BYTES) {
    override fun asBytes(value: Long): ByteArray = ByteBuffer.allocate(byteSize).also {
        it.putLong(value)
    }.array()

    override fun fromBytes(bytes: ByteArray) = ByteBuffer.wrap(bytes).long to Long.SIZE_BYTES

    override fun defaultValue(): Long = 0L

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        return true
    }

    override fun hashCode(): Int {
        return javaClass.hashCode()
    }

    override fun toString(): String {
        return "Long"
    }
}
fun longField(value: Long = 0L) = LongAttribute() to value

class StringAttribute: AttributeType<String>() {
    override fun asBytes(value: String): ByteArray =
        value.toCharArray().let {chars ->
            ByteBuffer.allocate(chars.size * Char.SIZE_BYTES + Int.SIZE_BYTES).also {
                var buf = it.putInt(chars.size)
                chars.forEach { c -> buf = buf.putChar(c) }
            }.array()
        }


    override fun fromBytes(bytes: ByteArray): Pair<String, Int> {
        val length = ByteBuffer.wrap(bytes).int * Char.SIZE_BYTES
        return ByteBuffer.wrap(bytes, Int.SIZE_BYTES, length).asCharBuffer().toString() to Int.SIZE_BYTES + length
    }

    override fun defaultValue(): String = ""

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        return true
    }

    override fun hashCode(): Int {
        return javaClass.hashCode()
    }

    override fun toString(): String {
        return "String"
    }

}
fun stringField(value: String = "") = StringAttribute() to value

class BooleanAttribute: AttributeType<Boolean>(Byte.SIZE_BYTES) {
    override fun asBytes(value: Boolean): ByteArray = ByteBuffer.allocate(byteSize).also {
        it.put(if (value) 1 else 0)
    }.array()

    override fun fromBytes(bytes: ByteArray) = (ByteBuffer.wrap(bytes).get().toInt() == 1) to Byte.SIZE_BYTES

    override fun defaultValue(): Boolean = false

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        return true
    }

    override fun hashCode(): Int {
        return javaClass.hashCode()
    }

    override fun toString(): String {
        return "Boolean"
    }
}
fun booleanField(value: Boolean = false) = BooleanAttribute() to value

class DoubleAttribute: AttributeType<Double>(Double.SIZE_BYTES) {
    override fun asBytes(value: Double): ByteArray = ByteBuffer.allocate(byteSize).also {
        it.putDouble(value)
    }.array()

    override fun fromBytes(bytes: ByteArray) = ByteBuffer.wrap(bytes).double to Double.SIZE_BYTES

    override fun defaultValue(): Double = 0.0

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        return true
    }

    override fun hashCode(): Int {
        return javaClass.hashCode()
    }

    override fun toString(): String {
        return "Double"
    }
}
fun doubleField(value: Double = 0.0) = DoubleAttribute() to value

class DateAttribute: AttributeType<Date>(Long.SIZE_BYTES) {
    override fun asBytes(value: Date): ByteArray = ByteBuffer.allocate(byteSize).also {
        it.putLong(value.time)
    }.array()

    override fun fromBytes(bytes: ByteArray) = ByteBuffer.wrap(bytes).long.let {
        Date.from(Instant.ofEpochMilli(it))
    } to Long.SIZE_BYTES

    override fun defaultValue(): Date = Date.from(Instant.EPOCH)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        return true
    }

    override fun hashCode(): Int {
        return javaClass.hashCode()
    }
    override fun toString(): String {
        return "Date"
    }
}
fun dateField(value: Date = Date.from(Instant.EPOCH)) = DateAttribute() to value

class Record3Attribute<T1: Any, T2: Any, T3: Any>(private val proto: Record3<T1, T2, T3>): AttributeType<Record3<T1, T2, T3>>() {
    override fun asBytes(value: Record3<T1, T2, T3>): ByteArray = value.asBytes()


    override fun fromBytes(bytes: ByteArray): Pair<Record3<T1, T2, T3>, Int> =
        proto.fromBytes(bytes).let {
            it to it.size
        }

    override fun defaultValue(): Record3<T1, T2, T3> = proto
}
fun <T1: Any, T2: Any, T3: Any> record3Field(value: Record3<T1, T2, T3>) = Record3Attribute(value) to value

// --------------------------------------------------------------------------------------------------------------------
// RecordN are n'ary tuples with typed components. Tuples can be serialized and deserialized to/from byte arrays.

class Record1<T1: Any>(f1: Pair<AttributeType<T1>, T1>) {
    val type1 = f1.first
    val value1 = f1.second

    val size: Int get() = asBytes().size

    fun asBytes(): ByteArray = type1.asBytes(value1)
    fun fromBytes(bytes: ByteArray): Record1<T1> = type1.fromBytes(bytes).let {
        Record1(type1 to it.first)
    }
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Record1<*>

        if (type1 != other.type1) return false
        if (value1 != other.value1) return false

        return true
    }

    override fun hashCode(): Int {
        var result = type1.hashCode()
        result = 31 * result + value1.hashCode()
        return result
    }
}

class Record2<T1: Any, T2: Any>(
    f1: Pair<AttributeType<T1>, T1>,
    f2: Pair<AttributeType<T2>, T2>) {


    val type1 = f1.first
    val value1 = f1.second
    val type2 = f2.first
    val value2 = f2.second

    val size: Int get() = asBytes().size
    fun asBytes(): ByteArray = type1.asBytes(value1) + type2.asBytes(value2)
    fun fromBytes(bytes: ByteArray): Record2<T1, T2> {
        val buffer = ByteBuffer.wrap(bytes)
        val v1: T1 = buffer.readAttribute(type1)
        val v2: T2 = buffer.readAttribute(type2)
        return Record2(type1 to v1, type2 to v2)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Record2<*, *>

        if (type1 != other.type1) return false
        if (value1 != other.value1) return false
        if (type2 != other.type2) return false
        if (value2 != other.value2) return false

        return true
    }

    override fun hashCode(): Int {
        var result = type1.hashCode()
        result = 31 * result + value1.hashCode()
        result = 31 * result + type2.hashCode()
        result = 31 * result + value2.hashCode()
        return result
    }

    override fun toString(): String {
        return "Record2(type1=$type1, value1=$value1, type2=$type2, value2=$value2)"
    }

    operator fun component1() = value1
    operator fun component2() = value2
}

class Record3<T1: Any, T2: Any, T3: Any>(
    val f1: Pair<AttributeType<T1>, T1>,
    val f2: Pair<AttributeType<T2>, T2>,
    val f3: Pair<AttributeType<T3>, T3>) {

    val type1 = f1.first
    val value1 = f1.second
    val type2 = f2.first
    val value2 = f2.second
    val type3 = f3.first
    val value3 = f3.second
    val size: Int get() = asBytes().size

    fun asBytes(): ByteArray = type1.asBytes(value1) + type2.asBytes(value2) + type3.asBytes(value3)
    fun fromBytes(bytes: ByteArray): Record3<T1, T2, T3> {
        val buffer = ByteBuffer.wrap(bytes)
        val v1: T1 = buffer.readAttribute(type1)
        val v2: T2 = buffer.readAttribute(type2)
        val v3: T3 = buffer.readAttribute(type3)
        return Record3(type1 to v1, type2 to v2, type3 to v3)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Record3<*, *, *>

        if (type1 != other.type1) return false
        if (value1 != other.value1) return false
        if (type2 != other.type2) return false
        if (value2 != other.value2) return false
        if (type3 != other.type3) return false
        if (value3 != other.value3) return false

        return true
    }

    override fun hashCode(): Int {
        var result = type1.hashCode()
        result = 31 * result + value1.hashCode()
        result = 31 * result + type2.hashCode()
        result = 31 * result + value2.hashCode()
        result = 31 * result + type3.hashCode()
        result = 31 * result + value3.hashCode()
        return result
    }

    override fun toString(): String {
        return "Record3($value1:$type1, $value2:$type2, $value3:$type3)"
    }

    operator fun component1() = value1
    operator fun component2() = value2

    operator fun component3() = value3
}

fun <T> ByteBuffer.readAttribute(attrType: AttributeType<T>): T {
    val (value, size) = attrType.fromBytes(this.slice().toBytes())
    this.position(this.position() + size)
    return value
}

private fun ByteBuffer.toBytes() = ByteArray(this.limit()).also {this.get(it)}

typealias TableRecordParser = (ByteArray) -> Record3<Any, Any, Any>
typealias AttributeValueParser<T> = Function<ByteArray, T>
