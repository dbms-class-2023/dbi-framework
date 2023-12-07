package net.barashev.dbi2023

/**
 * Lev Leontev
 */
class AgingPageCacheImpl(
    storage: Storage,
    maxCacheSize: Int = -1,
    val readsPerClockTick: Int = (maxCacheSize / 10).coerceAtLeast(1),
) : SimplePageCacheImpl(storage, maxCacheSize) {

    private var readsUntilClockTick: Int = readsPerClockTick
    private val pageAgeCounter = linkedMapOf<CachedPageImpl, UInt>()

    init {
        require(readsPerClockTick > 0)
    }

    override fun getEvictCandidate(): CachedPageImpl {
        val evictCandidate = pageAgeCounter
            .asSequence()
            .filter { (page, _) -> page.pinCount == 0 }
            .minByOrNull { (_, ageCounter) -> ageCounter }
            ?.key ?: throw IllegalStateException("All pages are pinned, there is no victim for eviction")
        pageAgeCounter.remove(evictCandidate)
        return evictCandidate
    }

    override fun onPageRequest(page: CachedPageImpl, isCacheHit: Boolean?) {
        super.onPageRequest(page, isCacheHit)
        incrementPageAgeCounter(page)
        clockTickIfNeeded()
    }

    private fun incrementPageAgeCounter(page: CachedPageImpl) {
        val counter = pageAgeCounter[page] ?: 0u
        pageAgeCounter[page] = counter or (1u shl 31)
    }

    private fun clockTickIfNeeded() {
        readsUntilClockTick -= 1
        if (readsUntilClockTick > 0) {
            return
        }
        clockTick()
        readsUntilClockTick = readsPerClockTick
    }

    private fun clockTick() {
        for (entry in pageAgeCounter.entries) {
            entry.setValue(entry.value shr 1)
        }
    }
}
