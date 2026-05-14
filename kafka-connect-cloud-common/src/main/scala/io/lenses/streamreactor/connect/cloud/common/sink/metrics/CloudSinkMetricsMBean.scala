/*
 * Copyright 2017-2026 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.cloud.common.sink.metrics

import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.LongAdder

/**
 * Reason tag for a forced master-lock write, recorded on the
 * `masterLockWriteForced*` counters.
 */
sealed trait ForcedWriteReason
object ForcedWriteReason {
  case object Revoke      extends ForcedWriteReason
  case object Stop        extends ForcedWriteReason
  case object PostCleanUp extends ForcedWriteReason
}

trait CloudSinkMetricsMBean {

  // --- Writer map ---

  /**
   * Current number of Writer instances in the writers map. Only active (non-idle) writers
   * remain in the map; idle writers are eagerly evicted when a new writer is created.
   */
  def getWriterCount: Int

  /**
   * Cumulative count of idle (NoWriter-state) writers evicted. Idle writers are eagerly
   * evicted whenever a new writer is created, keeping the map lean and allowing granular
   * lock GC to proceed sooner.
   */
  def getIdleWriterEvictions: Long

  // --- Granular lock cache ---

  /**
   * Current number of entries in the unbounded ConcurrentHashMap granular lock cache.
   * Each entry holds an eTag and committed offset for one partition key. A large value
   * signals high cardinality and proportional heap usage.
   */
  def getGranularCacheSize: Int

  /**
   * Cumulative cache hits when looking up a granular lock (eTag + offset). A high
   * hit ratio means writers are reusing cached eTags efficiently, avoiding cloud
   * storage reads on every commit.
   */
  def getGranularCacheHits: Long

  /**
   * Cumulative cache misses, each triggering a cloud storage GET to load the granular
   * lock. A spike indicates many new or re-created partition keys (e.g. after idle-writer
   * eviction or a rebalance).
   */
  def getGranularCacheMisses: Long

  // --- GC ---

  /**
   * Current number of obsolete granular lock file paths waiting in the gcQueue for
   * background deletion. A persistently high value suggests the drain interval
   * (gc.interval.seconds) or batch size (gc.batch.size) needs tuning.
   */
  def getGcQueueDepth: Int

  /**
   * Cumulative number of obsolete granular lock paths enqueued for deletion during
   * preCommit. Shows how much GC work is being generated over the task's lifetime.
   */
  def getGcLocksEnqueued: Long

  /**
   * Cumulative number of granular lock files actually deleted by the background GC drain.
   * Compare with [[getGcLocksEnqueued]] to verify GC is keeping up; a growing gap indicates
   * the drain cannot keep pace with enqueue rate.
   */
  def getGcLocksDeleted: Long

  /**
   * Cumulative count of enqueued locks skipped at drain time because a new writer reclaimed
   * the partition key between enqueue and drain. This is expected race-condition protection
   * and indicates healthy operation, not a problem.
   */
  def getGcLocksSkippedReclaimed: Long

  /**
   * Cumulative count of enqueued locks skipped because the topic-partition was revoked
   * (rebalanced away) before the background drain ran. Expected during rebalances.
   */
  def getGcLocksSkippedRevoked: Long

  /**
   * Cumulative failed batch-delete attempts (logged at WARN). Orphaned lock files are
   * harmless and will be re-enqueued, but a sustained count may indicate storage
   * permission or connectivity issues.
   */
  def getGcDeleteFailures: Long

  /**
   * Cumulative retried delete operations across all GC drain cycles. Elevated values
   * suggest transient cloud storage errors (throttling, timeouts).
   */
  def getGcDeleteRetries: Long

  // --- Master lock ---

  /**
   * Cumulative successful eTag-conditional master-lock writes (routine and forced combined).
   * Forced writes additionally increment the per-reason `masterLockWriteForced*` counter;
   * do NOT sum `masterLockUpdates + masterLockWriteForced*` — that double-counts forced
   * successes. For decomposition formulas see docs/datalake-exactly-once-partitionby.md
   * ("Master-Lock Write-Frequency Reduction and Metrics").
   */
  def getMasterLockUpdates: Long

  /**
   * Cumulative failed master lock writes. A failure means preCommit returned no offset for
   * the partition, so Kafka does not advance the consumer offset. Persistent failures may
   * indicate a zombie task holding a conflicting eTag or cloud storage issues.
   */
  def getMasterLockFailures: Long

  /**
   * Cumulative `preCommit` cycles where the dirty-flag gate suppressed the master-lock write
   * (`globalSafeOffset` had not advanced). The ratio
   * `masterLockWriteSkipped / (masterLockWriteSkipped + masterLockUpdates + masterLockFailures)`
   * measures the cost saving.
   */
  def getMasterLockWriteSkipped: Long

  /** Cumulative `WriteForced(Revoke)` attempts (partition revocation / rebalance). Records attempts, not successes. */
  def getMasterLockWriteForcedRevoke: Long

  /** Cumulative `WriteForced(Stop)` attempts (task stop / shutdown). Records attempts, not successes. */
  def getMasterLockWriteForcedStop: Long

  /**
   * Cumulative `WriteForced(PostCleanUp)` attempts: the first `preCommit` after a `cleanUp(tp)`
   * in-place rollback forces a write to persist `globalSafeOffset`. Records attempts, not
   * successes. See docs/datalake-exactly-once-partitionby.md ("`lastReturnedSafeOffset` Role by Mode").
   */
  def getMasterLockWriteForcedPostCleanUp: Long

  /**
   * Cumulative forced `WriteForced(Revoke)` write failures — a strict subset of
   * [[getMasterLockFailures]]. Incremented for both an expected `Left(err)` return
   * from `updateMasterLock` and an unexpected `NonFatal` throw on the forced path.
   */
  def getMasterLockWriteForcedRevokeFailures: Long

  /**
   * Cumulative forced `WriteForced(Stop)` write failures — a strict subset of
   * [[getMasterLockFailures]]. Incremented for both an expected `Left(err)` return
   * from `updateMasterLock` and an unexpected `NonFatal` throw on the forced path.
   */
  def getMasterLockWriteForcedStopFailures: Long

  /**
   * Cumulative forced `WriteForced(PostCleanUp)` write failures — a strict subset of
   * [[getMasterLockFailures]]. Incremented for both an expected `Left(err)` return
   * from `updateMasterLock` and an unexpected `NonFatal` throw on the forced path.
   */
  def getMasterLockWriteForcedPostCleanUpFailures: Long

  /**
   * Cumulative `preCommit` cycles where the dirty flag was true on entry. At most one per
   * `globalSafeOffset` advance; sustained elevation indicates `updateMasterLock` failures
   * have widened the dirty window beyond a single cycle.
   */
  def getMasterLockDirtyWindowCycles: Long

  /**
   * Point-in-time indicator: `true` iff at least one topic-partition was observed
   * `dirty` (`globalSafeOffset > lastWrittenMaster`) at its most recent `preCommit`
   * and has not yet had a successful master-lock write or been cleaned up since.
   * Pairs with [[getMasterLockDirtyWindowCycles]]: the counter is a rate signal,
   * this gauge is the stuck signal — non-zero across many scrapes means the dirty
   * window is not closing (e.g. sustained cloud outage, persistent eTag conflict).
   */
  def getMasterLockDirty: Boolean

  // --- Orphan sweep ---

  /**
   * Cumulative number of orphan-sweep cycles that have executed. Useful for confirming
   * the sweep timer (gc.sweep.interval.seconds) is firing at the expected cadence.
   */
  def getSweepRuns: Long

  /**
   * Cumulative orphaned granular lock files discovered by the sweep and enqueued for
   * deletion. Shows how much stale state is accumulating from prior task runs with
   * different partition-key distributions.
   */
  def getSweepOrphansEnqueued: Long

  /**
   * Number of cloud storage GET requests consumed in the most recent sweep cycle (reset
   * each cycle). Compare against gc.sweep.max.reads to gauge budget utilisation; if
   * consistently at the cap, increase the budget or shorten the sweep interval.
   */
  def getSweepGetBudgetUsed: Int

  /**
   * Last-call snapshot of `firstBufferedOffsets.size` recorded inside
   * [[io.lenses.streamreactor.connect.cloud.common.sink.writer.WriterManager.preCommit]] —
   * the number of writers currently holding the safe-offset barrier (i.e. writers in
   * Writing/Uploading state that have buffered but uncommitted records). With selective
   * commit, `globalSafeOffset` is bounded by the slowest writer's `firstBufferedOffset`;
   * this gauge surfaces that hold-back to operators. A persistently non-zero value with
   * stagnant Kafka offsets indicates a low-throughput partition key whose `flush.interval`
   * needs tuning (see the Operational Constraints section of `datalake-exactly-once-partitionby`).
   */
  def getSafeOffsetBarrierWriters: Int
}

class CloudSinkMetrics() extends CloudSinkMetricsMBean {

  private val writerCount         = new AtomicInteger(0)
  private val idleWriterEvictions = new LongAdder()

  private val granularCacheSize   = new AtomicInteger(0)
  private val granularCacheHits   = new LongAdder()
  private val granularCacheMisses = new LongAdder()

  private val gcQueueDepth            = new AtomicInteger(0)
  private val gcLocksEnqueued         = new LongAdder()
  private val gcLocksDeleted          = new LongAdder()
  private val gcLocksSkippedReclaimed = new LongAdder()
  private val gcLocksSkippedRevoked   = new LongAdder()
  private val gcDeleteFailures        = new LongAdder()
  private val gcDeleteRetries         = new LongAdder()

  private val masterLockUpdates                        = new LongAdder()
  private val masterLockFailures                       = new LongAdder()
  private val masterLockWriteSkipped                   = new LongAdder()
  private val masterLockWriteForcedRevoke              = new LongAdder()
  private val masterLockWriteForcedStop                = new LongAdder()
  private val masterLockWriteForcedPostCleanUp         = new LongAdder()
  private val masterLockWriteForcedRevokeFailures      = new LongAdder()
  private val masterLockWriteForcedStopFailures        = new LongAdder()
  private val masterLockWriteForcedPostCleanUpFailures = new LongAdder()
  private val masterLockDirtyWindowCycles              = new LongAdder()
  private val dirtyTopicPartitions =
    java.util.concurrent.ConcurrentHashMap.newKeySet[TopicPartition]()

  private val sweepRuns            = new LongAdder()
  private val sweepOrphansEnqueued = new LongAdder()
  private val sweepGetBudgetUsed   = new AtomicInteger(0)

  private val safeOffsetBarrierWriters = new AtomicInteger(0)

  // --- getters (exposed via JMX) ---

  override def getWriterCount:         Int  = writerCount.get()
  override def getIdleWriterEvictions: Long = idleWriterEvictions.sum()

  override def getGranularCacheSize:   Int  = granularCacheSize.get()
  override def getGranularCacheHits:   Long = granularCacheHits.sum()
  override def getGranularCacheMisses: Long = granularCacheMisses.sum()

  override def getGcQueueDepth:            Int  = gcQueueDepth.get()
  override def getGcLocksEnqueued:         Long = gcLocksEnqueued.sum()
  override def getGcLocksDeleted:          Long = gcLocksDeleted.sum()
  override def getGcLocksSkippedReclaimed: Long = gcLocksSkippedReclaimed.sum()
  override def getGcLocksSkippedRevoked:   Long = gcLocksSkippedRevoked.sum()
  override def getGcDeleteFailures:        Long = gcDeleteFailures.sum()
  override def getGcDeleteRetries:         Long = gcDeleteRetries.sum()

  override def getMasterLockUpdates:                        Long    = masterLockUpdates.sum()
  override def getMasterLockFailures:                       Long    = masterLockFailures.sum()
  override def getMasterLockWriteSkipped:                   Long    = masterLockWriteSkipped.sum()
  override def getMasterLockWriteForcedRevoke:              Long    = masterLockWriteForcedRevoke.sum()
  override def getMasterLockWriteForcedStop:                Long    = masterLockWriteForcedStop.sum()
  override def getMasterLockWriteForcedPostCleanUp:         Long    = masterLockWriteForcedPostCleanUp.sum()
  override def getMasterLockWriteForcedRevokeFailures:      Long    = masterLockWriteForcedRevokeFailures.sum()
  override def getMasterLockWriteForcedStopFailures:        Long    = masterLockWriteForcedStopFailures.sum()
  override def getMasterLockWriteForcedPostCleanUpFailures: Long    = masterLockWriteForcedPostCleanUpFailures.sum()
  override def getMasterLockDirtyWindowCycles:              Long    = masterLockDirtyWindowCycles.sum()
  override def getMasterLockDirty:                          Boolean = !dirtyTopicPartitions.isEmpty

  override def getSweepRuns:            Long = sweepRuns.sum()
  override def getSweepOrphansEnqueued: Long = sweepOrphansEnqueued.sum()
  override def getSweepGetBudgetUsed:   Int  = sweepGetBudgetUsed.get()

  override def getSafeOffsetBarrierWriters: Int = safeOffsetBarrierWriters.get()

  // --- mutators (not exposed via JMX trait) ---

  def setWriterCount(count: Int): Unit = writerCount.set(count)
  def incrementIdleWriterEvictions(): Unit = idleWriterEvictions.increment()

  def setGranularCacheSize(size: Int): Unit = granularCacheSize.set(size)
  def incrementGranularCacheHits():   Unit = granularCacheHits.increment()
  def incrementGranularCacheMisses(): Unit = granularCacheMisses.increment()

  def setGcQueueDepth(depth:          Int):  Unit = gcQueueDepth.set(depth)
  def incrementGcLocksEnqueued(count: Long): Unit = gcLocksEnqueued.add(count)
  def incrementGcLocksDeleted(count:  Long): Unit = gcLocksDeleted.add(count)
  def incrementGcLocksSkippedReclaimed(): Unit = gcLocksSkippedReclaimed.increment()
  def incrementGcLocksSkippedRevoked():   Unit = gcLocksSkippedRevoked.increment()
  def incrementGcDeleteFailures():        Unit = gcDeleteFailures.increment()
  def incrementGcDeleteRetries(count: Long): Unit = gcDeleteRetries.add(count)

  def incrementMasterLockUpdates():  Unit = masterLockUpdates.increment()
  def incrementMasterLockFailures(): Unit = masterLockFailures.increment()

  def incrementMasterLockWriteSkipped(): Unit = masterLockWriteSkipped.increment()

  def incrementMasterLockWriteForced(reason: ForcedWriteReason): Unit = reason match {
    case ForcedWriteReason.Revoke      => masterLockWriteForcedRevoke.increment()
    case ForcedWriteReason.Stop        => masterLockWriteForcedStop.increment()
    case ForcedWriteReason.PostCleanUp => masterLockWriteForcedPostCleanUp.increment()
  }

  def incrementMasterLockWriteForcedFailure(reason: ForcedWriteReason): Unit = reason match {
    case ForcedWriteReason.Revoke      => masterLockWriteForcedRevokeFailures.increment()
    case ForcedWriteReason.Stop        => masterLockWriteForcedStopFailures.increment()
    case ForcedWriteReason.PostCleanUp => masterLockWriteForcedPostCleanUpFailures.increment()
  }

  def incrementMasterLockDirtyWindowCycle(): Unit = masterLockDirtyWindowCycles.increment()

  def markMasterLockDirty(tp: TopicPartition): Unit = { val _ = dirtyTopicPartitions.add(tp) }
  def clearMasterLockDirty(tp: TopicPartition): Unit = { val _ = dirtyTopicPartitions.remove(tp) }
  def clearAllMasterLockDirty(): Unit = dirtyTopicPartitions.clear()

  def incrementSweepRuns(): Unit = sweepRuns.increment()
  def incrementSweepOrphansEnqueued(count: Long): Unit = sweepOrphansEnqueued.add(count)
  def setSweepGetBudgetUsed(used:          Int):  Unit = sweepGetBudgetUsed.set(used)

  def setSafeOffsetBarrierWriters(count: Int): Unit = safeOffsetBarrierWriters.set(count)
}
