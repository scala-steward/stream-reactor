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
   * Cumulative successful eTag-conditional master lock writes. Each success advances the
   * consumer-committable offset (globalSafeOffset) for a Kafka partition.
   *
   * This is the UNIVERSAL success counter: every successful write — whether classified as
   * `WriteRoutine` (dirty-flag gate admitted a normal cycle) or `WriteForced` (lifecycle
   * force point at revoke / stop / post-`cleanUp`) — increments this counter. Forced writes
   * ADDITIONALLY increment their per-reason attempt counter (`masterLockWriteForcedRevoke`,
   * `masterLockWriteForcedStop`, or `masterLockWriteForcedPostCleanUp`).
   *
   * IMPORTANT — the per-reason `masterLockWriteForced*` counters track ATTEMPTS, not
   * successes. A forced write that fails increments the per-reason counter but NOT this
   * counter (because `masterLockUpdates` is a success-only counter). This means the
   * decomposition below is exact only when all forced writes succeed. When forced writes
   * can fail (transient cloud error, eTag mismatch on a rapid rebalance), use
   * `masterLockFailures` to account for the difference:
   *
   *   forced_successes  = masterLockWriteForcedRevoke
   *                       + masterLockWriteForcedStop
   *                       + masterLockWriteForcedPostCleanUp
   *                       - (forced_failures, not directly exposed — infer via masterLockFailures)
   *   routine_successes ≈ masterLockUpdates - forced_successes
   *
   * For an exact split without inference: compare `masterLockUpdates` (all successes) with
   * `masterLockFailures` (all failures, routine and forced combined) and the per-reason
   * attempt counters.
   *
   * NOTE: do NOT aggregate as `masterLockUpdates + masterLockWriteForced*` — that would
   * double-count every forced success, since forced writes increment BOTH this counter and
   * their per-reason counter.
   */
  def getMasterLockUpdates: Long

  /**
   * Cumulative failed master lock writes. A failure means preCommit returned no offset for
   * the partition, so Kafka does not advance the consumer offset. Persistent failures may
   * indicate a zombie task holding a conflicting eTag or cloud storage issues.
   */
  def getMasterLockFailures: Long

  /**
   * Cumulative count of `preCommit` cycles for PARTITIONBY topic-partitions where the
   * dirty-flag gate suppressed the master-lock write because `globalSafeOffset` had not
   * advanced beyond the last successfully persisted value. The cycle still returned the
   * same safe offset to Kafka Connect (no cloud call was attempted). Operators read
   * this counter alongside `masterLockUpdates` and `masterLockFailures` to verify that
   * the dirty-flag gate is suppressing the writes they expect. The ratio
   * `masterLockWriteSkipped / (masterLockWriteSkipped + masterLockUpdates + masterLockFailures)`
   * is the direct measurement of the cost saving from the gate.
   */
  def getMasterLockWriteSkipped: Long

  /**
   * Cumulative count of master-lock writes attempted via the `WriteForced` classification
   * triggered by a partition revocation / rebalance close. Forced writes still use the
   * eTag-conditional path and may fail; this counter records only the attempt, mirroring
   * `masterLockUpdates` / `masterLockFailures` for the post-success and post-failure paths.
   */
  def getMasterLockWriteForcedRevoke: Long

  /**
   * Cumulative count of master-lock writes attempted via the `WriteForced` classification
   * triggered by task stop / shutdown. Best-effort and non-blocking: a transient or fencing
   * failure here is acceptable and the next task instance reads the older durable master
   * lock on restart.
   */
  def getMasterLockWriteForcedStop: Long

  /**
   * Cumulative count of master-lock writes attempted via the `WriteForced` classification
   * triggered by the post-`cleanUp(tp)` (fatal rollback) re-entry. After the in-place
   * rollback the next `preCommit` cycle for the still-owned partition forces a write to
   * persist `globalSafeOffset`, computed as `max(durableMasterFloor + 1, lastReturnedSafeOffset(tp))`.
   * In PARTITIONBY mode the two terms are equal in production (because `IndexManagerV2.updateMasterLock`
   * keeps `seekedOffsets(tp)` in lockstep with each successful cloud write); in non-PARTITIONBY
   * mode the `lastReturnedSafeOffset(tp)` term is load-bearing and can be strictly higher. The
   * `max` exists as the defence against future `IndexManager` implementations that decouple the two.
   */
  def getMasterLockWriteForcedPostCleanUp: Long

  /**
   * Cumulative count of `preCommit` cycles that observed `dirtyMasterLock(tp) == true` on
   * entry — i.e. cycles whose previously computed `globalSafeOffset` had not yet been
   * persisted to the master lock. Under the dirty-flag gate's success path this equals at
   * most one per advance; sustained non-zero values across many cycles indicate
   * `updateMasterLock`
   * is failing across multiple retries and the dirty window has widened beyond a single
   * cycle.
   */
  def getMasterLockDirtyWindowCycles: Long

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
   * Cumulative number of [[io.lenses.streamreactor.connect.cloud.common.sink.writer.WriterCommitManager]]
   * commit-cycle invocations (one per `commitFlushableWriters`, `commitFlushableWritersForTopicPartition`,
   * `commitPending`, or `commitForTopicPartition` call). Read together with
   * [[getSelectiveCommitWritersCommitted]] and [[getSelectiveCommitAvoidedSiblingCommits]] to derive
   * average fan-out per cycle without needing per-record sampling.
   */
  def getSelectiveCommitInvocations: Long

  /**
   * Cumulative number of writers actually committed across all selective-commit cycles
   * (i.e. the size of the selected set, summed). Combined with [[getSelectiveCommitInvocations]]
   * this yields the average number of writers committed per trigger.
   */
  def getSelectiveCommitWritersCommitted: Long

  /**
   * Cumulative count of sibling writers NOT committed thanks to selective commit — the
   * headline benefit counter. Computed per cycle as
   * `(writers whose topicPartition is in selected.map(_._1.topicPartition).toSet) - selected.size`,
   * i.e. the number of writers that would have been pulled in by the previous fan-out behaviour
   * but are now correctly left untouched. A persistently low value on a high-cardinality
   * PARTITIONBY workload suggests the trigger is firing on most siblings simultaneously
   * (e.g. time-based flush is too coarse for the partition-key churn rate).
   */
  def getSelectiveCommitAvoidedSiblingCommits: Long

  /**
   * Last-call snapshot of the number of writers selected for commit by the trigger
   * (i.e. `selected.size`). Together with [[getActiveWritersPerTopicPartition]] this is
   * a quick visual indicator of the current fan-out shape; reset on every commit cycle.
   */
  def getSelectedWritersPerFlush: Int

  /**
   * Last-call snapshot of the number of writers on the topic-partitions touched by the
   * most recent commit cycle (the would-have-been-fanned-out set). Reset on every cycle.
   */
  def getActiveWritersPerTopicPartition: Int

  /**
   * Last-call snapshot of `selected.size / (writers in matching topic-partitions)` — the
   * fraction of writers in the affected topic-partitions that were actually committed.
   * Defined to be 1.0 when the matching set is empty (degenerate; no benefit to report).
   * Lower values mean selective commit is avoiding more sibling fan-out; the plan's
   * benefit-measurement target is a median ≤ 0.35 on a high-cardinality 1-of-N fixture.
   */
  def getFlushFanOutRatio: Double

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

  private val masterLockUpdates                = new LongAdder()
  private val masterLockFailures               = new LongAdder()
  private val masterLockWriteSkipped           = new LongAdder()
  private val masterLockWriteForcedRevoke      = new LongAdder()
  private val masterLockWriteForcedStop        = new LongAdder()
  private val masterLockWriteForcedPostCleanUp = new LongAdder()
  private val masterLockDirtyWindowCycles      = new LongAdder()

  private val sweepRuns            = new LongAdder()
  private val sweepOrphansEnqueued = new LongAdder()
  private val sweepGetBudgetUsed   = new AtomicInteger(0)

  // Selective commit: counters accumulate; gauges hold the last-call snapshot. The gauges
  // intentionally describe one cycle and are not aggregated, so operators can correlate
  // them directly with a specific flush trigger without averaging-window confusion.
  private val selectiveCommitInvocations           = new LongAdder()
  private val selectiveCommitWritersCommitted      = new LongAdder()
  private val selectiveCommitAvoidedSiblingCommits = new LongAdder()

  private val selectedWritersPerFlush        = new AtomicInteger(0)
  private val activeWritersPerTopicPartition = new AtomicInteger(0)
  // flushFanOutRatio is a Double; AtomicLong of doubleToRawLongBits gives lock-free updates.
  private val flushFanOutRatioBits = new java.util.concurrent.atomic.AtomicLong(
    java.lang.Double.doubleToRawLongBits(1.0d),
  )
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

  override def getMasterLockUpdates:                Long = masterLockUpdates.sum()
  override def getMasterLockFailures:               Long = masterLockFailures.sum()
  override def getMasterLockWriteSkipped:           Long = masterLockWriteSkipped.sum()
  override def getMasterLockWriteForcedRevoke:      Long = masterLockWriteForcedRevoke.sum()
  override def getMasterLockWriteForcedStop:        Long = masterLockWriteForcedStop.sum()
  override def getMasterLockWriteForcedPostCleanUp: Long = masterLockWriteForcedPostCleanUp.sum()
  override def getMasterLockDirtyWindowCycles:      Long = masterLockDirtyWindowCycles.sum()

  override def getSweepRuns:            Long = sweepRuns.sum()
  override def getSweepOrphansEnqueued: Long = sweepOrphansEnqueued.sum()
  override def getSweepGetBudgetUsed:   Int  = sweepGetBudgetUsed.get()

  override def getSelectiveCommitInvocations:           Long = selectiveCommitInvocations.sum()
  override def getSelectiveCommitWritersCommitted:      Long = selectiveCommitWritersCommitted.sum()
  override def getSelectiveCommitAvoidedSiblingCommits: Long = selectiveCommitAvoidedSiblingCommits.sum()

  override def getSelectedWritersPerFlush:        Int    = selectedWritersPerFlush.get()
  override def getActiveWritersPerTopicPartition: Int    = activeWritersPerTopicPartition.get()
  override def getFlushFanOutRatio:               Double = java.lang.Double.longBitsToDouble(flushFanOutRatioBits.get())
  override def getSafeOffsetBarrierWriters:       Int    = safeOffsetBarrierWriters.get()

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

  /** Increment the skipped-write counter — a `preCommit` cycle whose dirty-flag gate suppressed the attempt. */
  def incrementMasterLockWriteSkipped(): Unit = masterLockWriteSkipped.increment()

  /**
   * Increment the forced-write counter for the corresponding lifecycle reason. The reason
   * is a hard-coded enum (`Revoke`, `Stop`, `PostCleanUp`) chosen at the call site;
   * a free-form tag would let the wrong reason leak into the metric stream .
   */
  def incrementMasterLockWriteForced(reason: ForcedWriteReason): Unit = reason match {
    case ForcedWriteReason.Revoke      => masterLockWriteForcedRevoke.increment()
    case ForcedWriteReason.Stop        => masterLockWriteForcedStop.increment()
    case ForcedWriteReason.PostCleanUp => masterLockWriteForcedPostCleanUp.increment()
  }

  /** Increment the dirty-window-cycle counter — incremented at the top of `preCommit` whenever `dirtyMasterLock(tp)` was already true. */
  def incrementMasterLockDirtyWindowCycle(): Unit = masterLockDirtyWindowCycles.increment()

  def incrementSweepRuns(): Unit = sweepRuns.increment()
  def incrementSweepOrphansEnqueued(count: Long): Unit = sweepOrphansEnqueued.add(count)
  def setSweepGetBudgetUsed(used:          Int):  Unit = sweepGetBudgetUsed.set(used)

  /**
   * Record one selective-commit cycle. `selectedSize` is the number of writers actually
   * committed; `matchingTpWriters` is the number of writers whose `topicPartition` matched
   * one of the selected writers' topic-partitions (the would-have-been-fanned-out set
   * under the previous step-2 expansion). Cumulative counters and last-call gauges are
   * updated together so a single read of the JMX bean is internally consistent.
   */
  def recordSelectiveCommit(selectedSize: Int, matchingTpWriters: Int): Unit = {
    selectiveCommitInvocations.increment()
    selectiveCommitWritersCommitted.add(selectedSize.toLong)
    val avoided = math.max(0, matchingTpWriters - selectedSize)
    selectiveCommitAvoidedSiblingCommits.add(avoided.toLong)
    selectedWritersPerFlush.set(selectedSize)
    activeWritersPerTopicPartition.set(matchingTpWriters)
    val ratio = if (matchingTpWriters <= 0) 1.0d else selectedSize.toDouble / matchingTpWriters.toDouble
    flushFanOutRatioBits.set(java.lang.Double.doubleToRawLongBits(ratio))
  }

  def setSafeOffsetBarrierWriters(count: Int): Unit = safeOffsetBarrierWriters.set(count)
}
