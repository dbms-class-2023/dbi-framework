package net.barashev.dbi2023

import java.nio.ByteBuffer
import kotlin.math.absoluteValue
import kotlin.math.sign

internal class DiskPageImpl(
    override val id: PageId,
    private val pageSize: Int,
    override val headerSize: Int = Int.SIZE_BYTES,
    private val bytes: ByteArray = ByteArray(pageSize)) : DiskPage {

    private var sourcePage: DiskPageImpl? = null
    constructor(sourcePage: DiskPageImpl): this(sourcePage.id, DEFAULT_DISK_PAGE_SIZE, sourcePage.headerSize, sourcePage.bytes.copyOf()) {
        this.sourcePage = sourcePage
    }

    private val directoryStartOffset = headerSize
    private var directorySize = 0
        set(value) {
            field = value
            this.bytes.setDirectorySize(value)
        }

    private var lastRecordOffset: Int = pageSize

    override val rawBytes: ByteArray get() = bytes
    private val byteBuffer: ByteBuffer get() = ByteBuffer.wrap(bytes, 0, pageSize)

    override val freeSpace: Int
        get() = lastRecordOffset - directorySize * Int.SIZE_BYTES - directoryStartOffset

    init {
        if (headerSize < Int.SIZE_BYTES) {
            throw IllegalArgumentException("Header size must be at least ${Int.SIZE_BYTES} bytes")
        }
        if (headerSize > pageSize) {
            throw IllegalArgumentException("Header size must be less than page size")
        }
        directorySize = this.bytes.getDirectorySize()
        lastRecordOffset = if (directorySize > 0) this.bytes.getDirectoryEntry(directorySize - 1).absoluteValue else pageSize
    }

    override fun putHeader(header: ByteArray) {
        ByteBuffer.wrap(bytes, 0, headerSize).put(header)
    }

    override fun getHeader(): ByteArray {
        return ByteBuffer.wrap(bytes, 0, headerSize).toBytes()
    }

    override fun putRecord(recordData: ByteArray, recordId: RecordId): PutRecordResult {
        val recordId_ = if (recordId == -1) directorySize else recordId
        return if (recordId_ < 0 || recordId_ > directorySize) {
            PutRecordResult(recordId_, isOutOfSpace = false, isOutOfRange = true)
        } else {
            if (recordId_ == directorySize) {
                if ((recordData.size + Int.SIZE_BYTES) > freeSpace) {
                    PutRecordResult(recordId_, isOutOfSpace = true, isOutOfRange = false)
                } else {
                    val newLastRecordOffset = lastRecordOffset - recordData.size
                    bytes.setDirectoryEntry(recordId_, newLastRecordOffset)
                    ByteBuffer.wrap(bytes, newLastRecordOffset, recordData.size).put(recordData)

                    lastRecordOffset = newLastRecordOffset
                    directorySize += 1
                    PutRecordResult(recordId_, isOutOfSpace = false, isOutOfRange = false)
                }
            } else {
                getByteBuffer(recordId_).let { bytes ->
                    val requiredSpace = recordData.size - bytes.first.capacity()
                    if (freeSpace < requiredSpace) {
                        PutRecordResult(recordId_, isOutOfSpace = true, isOutOfRange = false)
                    } else {
                        shiftRecords(recordId_, requiredSpace)
                        getByteBuffer(recordId_).let { newBytes ->
                            assert(newBytes.first.capacity() == recordData.size)
                            newBytes.first.put(recordData)
                            this.bytes.setDirectoryEntry(recordId_, newBytes.first.arrayOffset())
                            PutRecordResult(recordId_, isOutOfSpace = false, isOutOfRange = false)
                        }
                    }
                }
            }
        }
    }

    override fun clear() {
        (0 until pageSize).forEach { bytes[it] = 0 }
        directorySize = 0
        lastRecordOffset = pageSize
    }

    override fun getRecord(recordId: RecordId): GetRecordResult =
        if (recordId < 0 || recordId >= directorySize) {
            GetRecordResult(EMPTY_BYTE_ARRAY, isDeleted = false, isOutOfRange = true)
        } else {
            getByteBuffer(recordId).let {buffer ->
                if (buffer.second) {
                    GetRecordResult(EMPTY_BYTE_ARRAY, isDeleted = true, isOutOfRange = false)
                } else {
                    GetRecordResult(buffer.first.toBytes(), isDeleted = false, isOutOfRange = false)
                }
            }
        }


    override fun deleteRecord(recordId: RecordId) {
        if (recordId in 0 until directorySize) {
            val recordOffset = bytes.getDirectoryEntry(recordId)
            if (recordOffset > 0) {
                bytes.setDirectoryEntry(recordId, -recordOffset)
            }
        }
    }

    override fun allRecords(): Map<RecordId, GetRecordResult> =
        (0 until directorySize)
            .mapNotNull { recordId -> getByteBuffer(recordId).let {
                recordId to GetRecordResult(it.first.toBytes(), isDeleted = it.second, isOutOfRange = false)
            }}
            .toMap()


    private fun getByteBuffer(recordId: RecordId): Pair<ByteBuffer,  Boolean> {
        val byteBuffer = ByteBuffer.wrap(bytes, 0, pageSize)
        val offset = bytes.getDirectoryEntry(recordId)
        val prevRecordOffset = if (recordId == 0) pageSize else bytes.getDirectoryEntry(recordId - 1)
        val slice = byteBuffer.slice(offset.absoluteValue, prevRecordOffset.absoluteValue - offset.absoluteValue)
        return slice to (offset < 0)
    }

    private fun shiftRecords(startRecordId: RecordId, requiredSpace: Int) {
        if (requiredSpace == 0) {
            return
        }
        val startRecordOffset = bytes.getDirectoryEntry(startRecordId).absoluteValue
        val shiftedBytes = byteBuffer.slice(lastRecordOffset, startRecordOffset - lastRecordOffset).toBytes()
        val newLastRecordOffset = lastRecordOffset - requiredSpace
        byteBuffer.put(newLastRecordOffset, shiftedBytes)
        if (requiredSpace < 0) {
            // We are freeing space, let's fill the new space with zeroes
            byteBuffer.put(lastRecordOffset, ByteArray(requiredSpace.absoluteValue))
        }
        for (i in startRecordId until directorySize) {
            val offset = this.bytes.getDirectoryEntry(i)
            // Preserve negative sign of deleted records.
            this.bytes.setDirectoryEntry(i, offset.sign * (offset.absoluteValue - requiredSpace))
        }
        lastRecordOffset = newLastRecordOffset
    }

    private fun ByteArray.getDirectoryEntry(idx: Int) =
        ByteBuffer.wrap(this, 0, pageSize).getInt(directoryStartOffset + idx * Int.SIZE_BYTES)

    private fun ByteArray.setDirectoryEntry(idx: Int, value: Int) =
        ByteBuffer.wrap(this, 0, pageSize).putInt(directoryStartOffset + idx * Int.SIZE_BYTES, value)

    private fun ByteArray.getDirectorySize() = ByteBuffer.wrap(this, 0, pageSize).getInt(0)
    private fun ByteArray.setDirectorySize(size: Int) = ByteBuffer.wrap(this, 0, pageSize).putInt(0, size)

    private fun ByteBuffer.toBytes() = ByteArray(this.limit()).also {this.get(it)}

    override fun reset() {
        this.sourcePage?.bytes?.copyInto(this.bytes)
    }
}

internal val EMPTY_BYTE_ARRAY = ByteArray(0)
internal val DEFAULT_DISK_PAGE_SIZE = 4096