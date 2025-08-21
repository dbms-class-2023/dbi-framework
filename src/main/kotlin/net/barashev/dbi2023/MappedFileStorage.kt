/*
 * Memory-mapped file based Storage implementation.
 */
package net.barashev.dbi2023

import java.io.IOException
import java.io.RandomAccessFile
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.TreeMap
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Consumer
import kotlin.math.max

/**
 * Factory to create a memory-mapped file backed storage.
 *
 * @param baseDir directory to keep segment files. If null, a temp directory will be created.
 * @param filePrefix segment file name prefix. Files will be named as "$filePrefix-<index>.seg".
 * @param segmentSizeBytes size of a single segment file. Default 16 MiB.
 */
fun createMappedFileStorage(
    baseDir: Path? = null,
    filePrefix: String = "dbi-segment",
    segmentSizeBytes: Int = 16 * 1024 * 1024,
    factory: DiskPageFactory = defaultDiskPageFactory
): Storage = MappedFileStorage(baseDir, filePrefix, segmentSizeBytes, factory)

private class MappedFileStorage(
    baseDir: Path?,
    private val filePrefix: String,
    private val segmentSizeBytes: Int,
    private val diskPageFactory: DiskPageFactory
) : Storage {

    private val pageSize = DEFAULT_DISK_PAGE_SIZE
    private val pagesPerSegment = segmentSizeBytes / pageSize

    private val directory: Path = baseDir ?: Files.createTempDirectory("dbi-mapped-storage")
    private val segments = ConcurrentHashMap<Int, MappedByteBuffer>()
    private val channels = ConcurrentHashMap<Int, FileChannel>()

    // Track max written page id to support startPageId == -1 semantics similar to HardDiskEmulatorStorage
    @Volatile private var maxPageId: Int = -1

    // simple access cost metric
    @Volatile private var accessCostMs: Double = 0.0
    override val totalAccessCost: Double get() = accessCostMs

    init {
        println("MappedFileStorage created, directory = $directory")
    }
    private fun segmentIndex(pageId: PageId): Int = pageId.floorDiv(pagesPerSegment)
    private fun offsetInSegment(pageId: PageId): Int = (pageId % pagesPerSegment) * pageSize

    private fun segPath(segIdx: Int): Path = directory.resolve("$filePrefix-$segIdx.seg")

    private fun ensureSegment(segIdx: Int): Pair<FileChannel, MappedByteBuffer> {
        channels[segIdx]?.let { ch ->
            val mbb = segments[segIdx]
            if (mbb != null) return ch to mbb
        }
        synchronized(this) {
            channels[segIdx]?.let { ch ->
                val mbb = segments[segIdx]
                if (mbb != null) return ch to mbb
            }
            val path = segPath(segIdx)
            if (Files.notExists(path)) {
                Files.createDirectories(path.parent)
                // Create and pre-size the segment file
                RandomAccessFile(path.toFile(), "rw").use { raf ->
                    raf.setLength(segmentSizeBytes.toLong())
                }
            } else {
                // Ensure file is at least segmentSizeBytes
                RandomAccessFile(path.toFile(), "rw").use { raf ->
                    if (raf.length() < segmentSizeBytes) raf.setLength(segmentSizeBytes.toLong())
                }
            }
            val ch = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE)
            val mbb = ch.map(FileChannel.MapMode.READ_WRITE, 0, segmentSizeBytes.toLong())
            channels[segIdx] = ch
            segments[segIdx] = mbb
            return ch to mbb
        }
    }

    override fun read(pageId: PageId): DiskPage {
        if (pageId < 0) throw IllegalArgumentException("Illegal page ID = $pageId")
        val page = diskPageFactory.createEmptyPage(pageId)
        val segIdx = segmentIndex(pageId)
        val off = offsetInSegment(pageId)
        val (_, mbb) = ensureSegment(segIdx)
        // Copy bytes from mapped buffer into page
        synchronized(mbb) {
            val oldPos = mbb.position()
            try {
                mbb.position(off)
                mbb.get(page.rawBytes, 0, pageSize)
            } finally {
                mbb.position(oldPos)
            }
        }
        // We don't know if the page was written before; reading zeroed bytes is fine for an empty page
        countRandomAccess()
        return diskPageFactory.createCopyPage(page)
    }

    override fun bulkRead(startPageId: PageId, numPages: Int, reader: Consumer<DiskPage>) {
        val realStart = if (startPageId == -1) nextPageId() else startPageId
        var pid = realStart
        repeat(numPages) {
            reader.accept(read(pid))
            pid++
        }
        countRandomAccess()
        countSeqScan(numPages)
    }

    override fun write(page: DiskPage) {
        if (page.id < 0) throw IllegalArgumentException("Illegal page ID = ${page.id}")
        val segIdx = segmentIndex(page.id)
        val off = offsetInSegment(page.id)
        val (_, mbb) = ensureSegment(segIdx)
        synchronized(mbb) {
            val oldPos = mbb.position()
            try {
                mbb.position(off)
                mbb.put(page.rawBytes, 0, pageSize)
            } finally {
                mbb.position(oldPos)
            }
        }
        updateMaxPageId(page.id)
        countRandomAccess()
    }

    private inner class BulkWriter(private val start: PageId) : BulkWrite {
        override val startPageId: PageId = start
        private var current = start
        private var count = 0

        override fun write(inPage: DiskPage): DiskPage {
            val pageToWrite = if (inPage.id == current) inPage else diskPageFactory.createCopyPage(inPage, current)
            this@MappedFileStorage.write(pageToWrite)
            count++
            return pageToWrite.also { current++ }
        }

        override fun close() {
            countSeqScan(count)
        }
    }

    override fun bulkWrite(startPageId: PageId): BulkWrite {
        val start = if (startPageId == -1) nextPageId() else startPageId
        if (start < 0) throw IllegalArgumentException("Illegal start page ID = $start")
        countRandomAccess()
        return BulkWriter(start)
    }

    private fun nextPageId(): Int = max(0, maxPageId + 1)
    private fun updateMaxPageId(pageId: Int) {
        while (true) {
            val cur = maxPageId
            if (pageId <= cur) return
            maxPageId = pageId
            return
        }
    }

    private fun countRandomAccess() { accessCostMs += 0.1 } // lightweight metric
    private fun countSeqScan(numPages: Int) { accessCostMs += 0.01 * numPages }
    override fun close() {
        // Flush all mapped buffers to the storage and close channels
        try {
            // Make a snapshot of entries to avoid concurrent modification during iteration
            val segSnapshot = segments.entries.toList()
            segSnapshot.forEach { entry ->
                val mbb = entry.value
                try {
                    synchronized(mbb) {
                        mbb.force()
                    }
                } catch (_: Throwable) {
                    // Best-effort flush; ignore to proceed closing others
                }
            }
        } finally {
            // Force and close all channels
            val chSnapshot = channels.entries.toList()
            chSnapshot.forEach { entry ->
                val ch = entry.value
                try {
                    ch.force(true)
                } catch (_: Throwable) {
                    // ignore
                }
                try {
                    ch.close()
                } catch (_: Throwable) {
                    // ignore
                }
            }
            segments.clear()
            channels.clear()
        }
    }

}
