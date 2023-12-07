package net.barashev.dbi2023

import java.util.function.Function
import kotlin.random.Random

/**
 * Veronika Sirotkina
 */
class BTreeIndex<T : Comparable<T>, S : AttributeType<T>>(
    private val rootPageId: PageId,
    private val depth: Int,
    private val pageCache: PageCache,
    private val keyType: S,
    private val recordParser: Function<ByteArray, Pair<T, PageId>>
) : Index<T> {
    private fun getRecords(page: CachedPage) = page.allRecords().filterValues { it.isOk }
        .map { recordParser.apply(it.value.bytes) }

    private fun getDenseRecords(page: CachedPage) = getRecords(page).let { it.subList(1, it.size) }

    private fun getDenseIndexPage(indexKey: T): CachedPage {
        var nextPage = pageCache.get(rootPageId)
        repeat(depth - 1) {
            val records = getRecords(nextPage)
            val record = records.lastOrNull { record ->
                record.first != keyType.defaultValue() && record.first <= indexKey
            } ?: records.first()
            nextPage = pageCache.get(record.second)
        }
        return nextPage
    }

    /**
     * Searches for the specified index key and returns an identifier of a table page where a record with such
     * value of the indexed attribute can be found.
     */
    override fun lookup(indexKey: T): List<PageId> {
        var denseIndexPage = getDenseIndexPage(indexKey)
        val result = mutableListOf<PageId>()
        var records = getDenseRecords(denseIndexPage)
        var firstRecordIndex = records.indexOfFirst { (key, _) -> key == indexKey }
        var lastRecordIndex = records.indexOfFirst { (key, _) ->
            key > indexKey && key != keyType.defaultValue()
        }.takeUnless { it == -1 } ?: records.size
        while (firstRecordIndex != -1) {
            result += records.slice(firstRecordIndex until lastRecordIndex).map { it.second }
            val nextPageId = recordParser.apply(denseIndexPage.getRecord(0).bytes).second
            if (nextPageId == Int.MIN_VALUE) {
                break
            }
            denseIndexPage = pageCache.get(nextPageId)
            records = getDenseRecords(denseIndexPage)
            firstRecordIndex = if (records[0].first == indexKey) 0 else -1
            lastRecordIndex = records.indexOfFirst { (key, _) ->
                key > indexKey
            }.takeUnless { it == -1 } ?: records.size
        }
        return result
    }
}

private class BTreeIndexBuilder<T : Comparable<T>, S : AttributeType<T>>(
    private val tableOid: Oid,
    private val storageAccessManager: StorageAccessManager,
    private val pageCache: PageCache,
    private val keyType: S
) {
    private data class TreeNode<T : Comparable<T>>(
        var minNewKey: T?,
        val indexPageId: PageId,
        val layerIndex: Int,
        var lastKey: T? = null,
        var isCleanNode: Boolean = true
    )

    private fun Pair<T?, PageId>.toRecord() = Record2(
        keyType to (first ?: keyType.defaultValue()),
        intField(second)
    ).asBytes()

    private fun TreeNode<T>.writeRecord(key: T?, pageId: PageId): Boolean {
        // In the dense index layer keys are not nullified
        val keyValue = if (layerIndex > 0 && (isCleanNode || key == lastKey)) null else key
        val page = pageCache.get(indexPageId)
        val putRecordResult = page.putRecord((keyValue to pageId).toRecord())
        if (putRecordResult.isOk) {
            if (minNewKey == null && key != lastKey) {
                minNewKey = key
            }
            lastKey = key
            isCleanNode = false
            return true
        }
        return false
    }

    // Build layers from bottom to top, for each layer store the newest page
    private val layers = mutableListOf<TreeNode<T>>()

    fun writeRecord(key: T, pageId: PageId) {
        writeRecordToLayer(key, pageId, 0)
    }

    fun finish() {
        for (i in 0 until layers.size - 1) {
            closePage(i)
        }
    }

    fun getRoot() = layers.last().indexPageId

    fun getDepth() = layers.size

    private fun addLayer() {
        layers.add(TreeNode(null, storageAccessManager.addPage(tableOid), layers.size))
        if (layers.size == 1) {
            linkDenseIndexPage(layers.first().indexPageId, Int.MIN_VALUE)
        }
    }

    /**
     * Notify the structure that nothing else will be written to the page.
     * Record the page on the next layer.
     */
    private fun closePage(layerIndex: Int) {
        assert(layers.size > 0)
        assert(layerIndex < layers.size)
        if (layerIndex == layers.size - 1) {
            addLayer()
        }
        val node = layers[layerIndex]
        // Update records on the next layer
        writeRecordToLayer(node.minNewKey, node.indexPageId, layerIndex + 1)
    }

    private fun linkDenseIndexPage(pageId: PageId, nextPageId: PageId) {
        val page = pageCache.get(pageId)
        assert(page.putRecord((keyType.defaultValue() to nextPageId).toRecord(), 0).isOk)
    }

    /**
     * Write record to the last open page in the layer. If the write fails, open a new page.
     */
    private fun writeRecordToLayer(key: T?, pageId: PageId, layerIndex: Int) {
        if (layerIndex == layers.size) {
            addLayer()
        }
        assert(layerIndex < layers.size)
        val node = layers[layerIndex]
        if (node.writeRecord(key, pageId)) {
            return
        }
        // Write failed
        closePage(layerIndex)
        val closedPageId = layers[layerIndex].indexPageId
        layers[layerIndex] = TreeNode(
            null, storageAccessManager.addPage(tableOid),
            layerIndex, node.lastKey
        )
        val newNode = layers[layerIndex]
        if (layerIndex == 0) {
            // Pages on the bottom layer must be in a linked list
            linkDenseIndexPage(closedPageId, newNode.indexPageId)
            linkDenseIndexPage(newNode.indexPageId, Int.MIN_VALUE)
        }
        assert(newNode.writeRecord(key, pageId))
    }
}

class BTreeIndexManager(
    private val storageAccessManager: StorageAccessManager,
    private val pageCache: PageCache
) : IndexManager {
    private val tableNameToIndexRootMapping = mutableMapOf<String, Pair<PageId, Int>>()

    /**
     * Builds index structure from the values produced with indexKey function from the records of tableName table.
     * Index pages shall be written as indexTableName pages to the storage.
     * keyType attribute instance may be used to serialize the index keys in the index records.
     *
     * Calling this method will create (and possible destroy previous) disk pages, so it shall be used with care.
     */
    override fun <T : Comparable<T>, S : AttributeType<T>> build(
        tableName: String,
        indexTableName: String,
        method: IndexMethod,
        isUnique: Boolean,
        keyType: S,
        indexKey: Function<ByteArray, T>
    ): Index<T> {
        println("created index table $indexTableName")
        // Scan the entire input table, sort the index keys and associate each key with the page id where the original
        // data record is located.
        // We write the sorted pairs of (indexKey, dataPageId) into the auxiliary table, and then sort it.
        val auxTableParser = { bytes: ByteArray ->
            Record2(keyType to keyType.defaultValue(), intField()).fromBytes(bytes)
        }
        val auxTableName = "__$tableName.aux${Random.nextLong()}__"
        TableBuilder(storageAccessManager, pageCache, storageAccessManager.createTable(auxTableName)).use { auxBuilder ->
            storageAccessManager.createFullScan(tableName).pages().forEach { page ->
                page.allRecords().filter { it.value.isOk }
                    .map { indexKey.apply(it.value.bytes) to page.id }.forEach {
                        auxBuilder.insert(Record2(keyType to it.first, intField(page.id)).asBytes())
                    }
            }
        }
        val sortedTableName = Operations.sortFactory(storageAccessManager, pageCache)
            .sort(auxTableName) {
                auxTableParser(it).value1
            }

        // Build index
        val indexTableId = storageAccessManager.createTable(indexTableName)
        val indexBuilder = BTreeIndexBuilder(indexTableId, storageAccessManager, pageCache, keyType)
        storageAccessManager.createFullScan(sortedTableName).records(auxTableParser)
            .forEach { record ->
                indexBuilder.writeRecord(record.component1(), record.component2())
            }
        indexBuilder.finish()
        storageAccessManager.deleteTable(auxTableName)
        storageAccessManager.deleteTable(sortedTableName)

        tableNameToIndexRootMapping[indexTableName] = Pair(indexBuilder.getRoot(), indexBuilder.getDepth())
        return BTreeIndex(indexBuilder.getRoot(), indexBuilder.getDepth(), pageCache, keyType) { record ->
            Record2(keyType to keyType.defaultValue(), intField()).fromBytes(record)
                .let { it.component1() to it.component2() }
        }
    }

    /**
     * Opens an index structure which was previously built from the values produced with indexKey function from the records of tableName table,
     * and written as indexTableName pages to the storage.
     * keyType attribute instance may be used to (de)serialize the index keys in the index records.
     *
     * This method per se is read-only, and won't modify the index pages.
     * @throws IndexException if there is no indexTableName table or if its pages are not a valid index of the specified index method.
     */
    override fun <T : Comparable<T>, S : AttributeType<T>> open(
        tableName: String,
        indexTableName: String,
        method: IndexMethod,
        isUnique: Boolean,
        keyType: S,
        indexKey: Function<ByteArray, T>
    ): Index<T> {
        if (method != IndexMethod.BTREE) {
            throw IndexException("BTreeIndexManager manages BTREE indexes, $method index was requested instead")
        }
        val (rootPageId, depth) = tableNameToIndexRootMapping[indexTableName]
                                  ?: throw IndexException("No such BTree index table: $indexTableName. What is there: $tableNameToIndexRootMapping")
        return BTreeIndex(rootPageId, depth, pageCache, keyType) { record ->
            Record2(keyType to keyType.defaultValue(), intField()).fromBytes(record)
                .let { it.component1() to it.component2() }
        }
    }

}