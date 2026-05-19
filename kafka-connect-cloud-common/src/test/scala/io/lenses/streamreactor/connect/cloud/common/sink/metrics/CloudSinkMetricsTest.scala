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

import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.doThrow
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach

import java.lang.management.ManagementFactory
import javax.management.InstanceAlreadyExistsException
import javax.management.MBeanServer
import javax.management.ObjectName

class CloudSinkMetricsTest extends AnyFunSuiteLike with Matchers with BeforeAndAfterEach {

  private var metrics: CloudSinkMetrics = _

  override def beforeEach(): Unit = {
    metrics = new CloudSinkMetrics()
    super.beforeEach()
  }

  // --- Writer map gauges / counters ---

  test("WriterCount defaults to 0 and can be set") {
    metrics.getWriterCount shouldBe 0
    metrics.setWriterCount(42)
    metrics.getWriterCount shouldBe 42
  }

  test("IdleWriterEvictions increments correctly") {
    metrics.getIdleWriterEvictions shouldBe 0L
    metrics.incrementIdleWriterEvictions()
    metrics.incrementIdleWriterEvictions()
    metrics.getIdleWriterEvictions shouldBe 2L
  }

  // --- Granular cache gauges / counters ---

  test("GranularCacheSize defaults to 0 and can be set") {
    metrics.getGranularCacheSize shouldBe 0
    metrics.setGranularCacheSize(100)
    metrics.getGranularCacheSize shouldBe 100
  }

  test("GranularCacheHits and Misses increment correctly") {
    metrics.getGranularCacheHits shouldBe 0L
    metrics.getGranularCacheMisses shouldBe 0L
    metrics.incrementGranularCacheHits()
    metrics.incrementGranularCacheHits()
    metrics.incrementGranularCacheMisses()
    metrics.getGranularCacheHits shouldBe 2L
    metrics.getGranularCacheMisses shouldBe 1L
  }

  // --- GC counters ---

  test("GcQueueDepth defaults to 0 and can be set") {
    metrics.getGcQueueDepth shouldBe 0
    metrics.setGcQueueDepth(7)
    metrics.getGcQueueDepth shouldBe 7
  }

  test("GcLocksEnqueued accumulates") {
    metrics.getGcLocksEnqueued shouldBe 0L
    metrics.incrementGcLocksEnqueued(5)
    metrics.incrementGcLocksEnqueued(3)
    metrics.getGcLocksEnqueued shouldBe 8L
  }

  test("GcLocksDeleted accumulates") {
    metrics.getGcLocksDeleted shouldBe 0L
    metrics.incrementGcLocksDeleted(10)
    metrics.getGcLocksDeleted shouldBe 10L
  }

  test("GcLocksSkippedReclaimed increments") {
    metrics.getGcLocksSkippedReclaimed shouldBe 0L
    metrics.incrementGcLocksSkippedReclaimed()
    metrics.getGcLocksSkippedReclaimed shouldBe 1L
  }

  test("GcDeleteFailures increments") {
    metrics.getGcDeleteFailures shouldBe 0L
    metrics.incrementGcDeleteFailures()
    metrics.incrementGcDeleteFailures()
    metrics.getGcDeleteFailures shouldBe 2L
  }

  // --- Master lock counters ---

  test("MasterLockUpdates and Failures increment correctly") {
    metrics.getMasterLockUpdates shouldBe 0L
    metrics.getMasterLockFailures shouldBe 0L
    metrics.incrementMasterLockUpdates()
    metrics.incrementMasterLockUpdates()
    metrics.incrementMasterLockFailures()
    metrics.getMasterLockUpdates shouldBe 2L
    metrics.getMasterLockFailures shouldBe 1L
  }

  // --- Sweep counters ---

  test("SweepRuns and SweepOrphansEnqueued increment correctly") {
    metrics.getSweepRuns shouldBe 0L
    metrics.getSweepOrphansEnqueued shouldBe 0L
    metrics.incrementSweepRuns()
    metrics.incrementSweepOrphansEnqueued(12)
    metrics.getSweepRuns shouldBe 1L
    metrics.getSweepOrphansEnqueued shouldBe 12L
  }

  test("SweepGetBudgetUsed defaults to 0 and can be set") {
    metrics.getSweepGetBudgetUsed shouldBe 0
    metrics.setSweepGetBudgetUsed(500)
    metrics.getSweepGetBudgetUsed shouldBe 500
  }

  // --- JMX registration round-trip ---

  test("register and unregister via CloudSinkMetricsRegistrar") {
    val taskId = ConnectorTaskId("test-connector", 2, 1)
    val name   = new ObjectName("io.lenses.streamreactor.connect.cloud.sink:type=metrics,name=test-connector,task=1")
    val mbs    = ManagementFactory.getPlatformMBeanServer

    try {
      CloudSinkMetricsRegistrar.register(metrics, taskId)
      mbs.isRegistered(name) shouldBe true

      metrics.setWriterCount(77)
      mbs.getAttribute(name, "WriterCount") shouldBe 77

      metrics.incrementMasterLockUpdates()
      mbs.getAttribute(name, "MasterLockUpdates") shouldBe 1L
    } finally {
      CloudSinkMetricsRegistrar.unregister(taskId)
    }
    mbs.isRegistered(name) shouldBe false
  }

  test("register throws IllegalStateException after exhausting retries on persistent InstanceAlreadyExistsException") {
    val taskId = ConnectorTaskId("retry-exhaust", 1, 0)
    val name =
      new ObjectName("io.lenses.streamreactor.connect.cloud.sink:type=metrics,name=retry-exhaust,task=0")
    val mbs = mock(classOf[MBeanServer])

    when(mbs.isRegistered(name)).thenReturn(false)
    doThrow(new InstanceAlreadyExistsException(name.toString))
      .when(mbs).registerMBean(any[Object], any[ObjectName])

    val thrown = intercept[IllegalStateException] {
      CloudSinkMetricsRegistrar.register(mbs, metrics, taskId)
    }

    thrown.getMessage should include(name.toString)
    thrown.getCause shouldBe a[InstanceAlreadyExistsException]
    verify(mbs, times(3)).registerMBean(any[Object], any[ObjectName])
  }

  test("register succeeds on retry when registerMBean throws InstanceAlreadyExistsException once then succeeds") {
    val taskId = ConnectorTaskId("retry-recover", 1, 0)
    val name =
      new ObjectName("io.lenses.streamreactor.connect.cloud.sink:type=metrics,name=retry-recover,task=0")
    val mbs = mock(classOf[MBeanServer])

    when(mbs.isRegistered(name)).thenReturn(false)
    doThrow(new InstanceAlreadyExistsException(name.toString))
      .doAnswer(_ => null)
      .when(mbs).registerMBean(any[Object], any[ObjectName])

    CloudSinkMetricsRegistrar.register(mbs, metrics, taskId)

    verify(mbs, times(2)).registerMBean(any[Object], any[ObjectName])
    verify(mbs, times(1)).unregisterMBean(name)
  }

  // =========================================================================
  // A. Ingest throughput
  // =========================================================================

  test("PutBatchesTotal defaults to 0 and increments") {
    metrics.getPutBatchesTotal shouldBe 0L
    metrics.incrementPutBatchesTotal()
    metrics.incrementPutBatchesTotal()
    metrics.getPutBatchesTotal shouldBe 2L
  }

  test("RecordsReceivedTotal accumulates batches") {
    metrics.getRecordsReceivedTotal shouldBe 0L
    metrics.addRecordsReceivedTotal(100L)
    metrics.addRecordsReceivedTotal(50L)
    metrics.getRecordsReceivedTotal shouldBe 150L
  }

  test("RecordsWrittenTotal increments") {
    metrics.getRecordsWrittenTotal shouldBe 0L
    metrics.incrementRecordsWrittenTotal()
    metrics.getRecordsWrittenTotal shouldBe 1L
  }

  test("NullRecordsSkippedTotal increments") {
    metrics.getNullRecordsSkippedTotal shouldBe 0L
    metrics.incrementNullRecordsSkippedTotal()
    metrics.getNullRecordsSkippedTotal shouldBe 1L
  }

  test("PutEmptyBatchesTotal increments") {
    metrics.getPutEmptyBatchesTotal shouldBe 0L
    metrics.incrementPutEmptyBatchesTotal()
    metrics.getPutEmptyBatchesTotal shouldBe 1L
  }

  test("LastPutEpochMillis defaults to 0 and can be set") {
    metrics.getLastPutEpochMillis shouldBe 0L
    val now = System.currentTimeMillis()
    metrics.setLastPutEpochMillis(now)
    metrics.getLastPutEpochMillis shouldBe now
  }

  test("PutTimer records count, sum, max, min, last") {
    metrics.getPutTimerCount shouldBe 0L
    metrics.recordPutTimer(10L)
    metrics.recordPutTimer(30L)
    metrics.recordPutTimer(20L)
    metrics.getPutTimerCount shouldBe 3L
    metrics.getPutTimerSumMillis shouldBe 60L
    metrics.getPutTimerMaxMillis shouldBe 30L
    metrics.getPutTimerMinMillis shouldBe 10L
    metrics.getPutTimerLastMillis shouldBe 20L
  }

  // =========================================================================
  // B. File / commit lifecycle
  // =========================================================================

  test("FilesOpenedTotal increments") {
    metrics.getFilesOpenedTotal shouldBe 0L
    metrics.incrementFilesOpenedTotal()
    metrics.getFilesOpenedTotal shouldBe 1L
  }

  test("FilesCommittedTotal and FilesFailedTotal track outcomes") {
    metrics.getFilesCommittedTotal shouldBe 0L
    metrics.getFilesFailedTotal shouldBe 0L
    metrics.incrementFilesCommittedTotal()
    metrics.incrementFilesCommittedTotal()
    metrics.incrementFilesFailedTotal()
    metrics.getFilesCommittedTotal shouldBe 2L
    metrics.getFilesFailedTotal shouldBe 1L
  }

  test("BytesWrittenTotal and RecordsCommittedTotal accumulate") {
    metrics.getBytesWrittenTotal shouldBe 0L
    metrics.getRecordsCommittedTotal shouldBe 0L
    metrics.addBytesWrittenTotal(1024L)
    metrics.addBytesWrittenTotal(512L)
    metrics.addRecordsCommittedTotal(100L)
    metrics.getBytesWrittenTotal shouldBe 1536L
    metrics.getRecordsCommittedTotal shouldBe 100L
  }

  test("FlushDecision counters track true/false decisions") {
    metrics.getFlushDecisionTrueTotal shouldBe 0L
    metrics.getFlushDecisionFalseTotal shouldBe 0L
    metrics.incrementFlushDecisionTrue()
    metrics.incrementFlushDecisionFalse()
    metrics.incrementFlushDecisionFalse()
    metrics.getFlushDecisionTrueTotal shouldBe 1L
    metrics.getFlushDecisionFalseTotal shouldBe 2L
  }

  test("CommitTimer records five stats") {
    metrics.getCommitTimerCount shouldBe 0L
    metrics.recordCommitTimer(5L)
    metrics.recordCommitTimer(15L)
    metrics.getCommitTimerCount shouldBe 2L
    metrics.getCommitTimerSumMillis shouldBe 20L
    metrics.getCommitTimerMaxMillis shouldBe 15L
    metrics.getCommitTimerMinMillis shouldBe 5L
  }

  test("MillisSinceLastCommit returns 0 before first commit then a positive value") {
    metrics.getMillisSinceLastCommit shouldBe 0L
    metrics.setLastCommitEpochMillis(System.currentTimeMillis() - 1000L)
    metrics.getMillisSinceLastCommit should be > 0L
  }

  // =========================================================================
  // C. Storage SDK timers
  // =========================================================================

  test("StorageUpload timer and error counter work independently") {
    metrics.getStorageUploadTimerCount shouldBe 0L
    metrics.getStorageUploadErrorsTotal shouldBe 0L
    metrics.recordStorageUpload(10L, isError = false)
    metrics.recordStorageUpload(20L, isError = true)
    metrics.getStorageUploadTimerCount shouldBe 2L
    metrics.getStorageUploadErrorsTotal shouldBe 1L
    metrics.getStorageUploadTimerMaxMillis shouldBe 20L
  }

  test("StorageCopy timer and error counter track independently") {
    metrics.recordStorageCopy(5L, isError = false)
    metrics.recordStorageCopy(5L, isError = true)
    metrics.getStorageCopyTimerCount shouldBe 2L
    metrics.getStorageCopyErrorsTotal shouldBe 1L
  }

  test("StorageDelete timer records both deleteFile and deleteFiles calls") {
    metrics.recordStorageDelete(3L, isError = false)
    metrics.recordStorageDelete(7L, isError = true)
    metrics.getStorageDeleteTimerCount shouldBe 2L
    metrics.getStorageDeleteErrorsTotal shouldBe 1L
    metrics.getStorageDeleteTimerSumMillis shouldBe 10L
  }

  test("StorageGet timer tracks getBlobAsStringAndEtag latency") {
    metrics.recordStorageGet(8L, isError = false)
    metrics.getStorageGetTimerCount shouldBe 1L
    metrics.getStorageGetErrorsTotal shouldBe 0L
    metrics.getStorageGetTimerLastMillis shouldBe 8L
  }

  test("StorageList timer tracks list operation latency") {
    metrics.recordStorageList(12L, isError = false)
    metrics.recordStorageList(6L, isError  = true)
    metrics.getStorageListTimerCount shouldBe 2L
    metrics.getStorageListErrorsTotal shouldBe 1L
    metrics.getStorageListTimerMinMillis shouldBe 6L
  }

  // =========================================================================
  // D. Retries & error classification
  // =========================================================================

  test("RecommitPendingInvocationsTotal increments") {
    metrics.getRecommitPendingInvocationsTotal shouldBe 0L
    metrics.incrementRecommitPendingInvocationsTotal()
    metrics.getRecommitPendingInvocationsTotal shouldBe 1L
  }

  test("PendingOperationRetriesTotal increments") {
    metrics.getPendingOperationRetriesTotal shouldBe 0L
    metrics.incrementPendingOperationRetriesTotal()
    metrics.getPendingOperationRetriesTotal shouldBe 1L
  }

  test("Sink error classification counters increment independently") {
    metrics.getSinkErrorsFatalTotal shouldBe 0L
    metrics.getSinkErrorsRetriableTotal shouldBe 0L
    metrics.getSinkErrorsNonFatalTotal shouldBe 0L
    metrics.incrementSinkErrorsFatalTotal()
    metrics.incrementSinkErrorsRetriableTotal()
    metrics.incrementSinkErrorsRetriableTotal()
    metrics.incrementSinkErrorsNonFatalTotal()
    metrics.getSinkErrorsFatalTotal shouldBe 1L
    metrics.getSinkErrorsRetriableTotal shouldBe 2L
    metrics.getSinkErrorsNonFatalTotal shouldBe 1L
  }

  // =========================================================================
  // E. Schema / skip / seek diagnostics
  // =========================================================================

  test("SchemaRolloversTotal increments") {
    metrics.getSchemaRolloversTotal shouldBe 0L
    metrics.incrementSchemaRolloversTotal()
    metrics.getSchemaRolloversTotal shouldBe 1L
  }

  test("DuplicateRecordsSkippedTotal increments") {
    metrics.getDuplicateRecordsSkippedTotal shouldBe 0L
    metrics.incrementDuplicateRecordsSkippedTotal()
    metrics.getDuplicateRecordsSkippedTotal shouldBe 1L
  }

  test("SeekOnOpenAppliedTotal increments") {
    metrics.getSeekOnOpenAppliedTotal shouldBe 0L
    metrics.incrementSeekOnOpenAppliedTotal()
    metrics.getSeekOnOpenAppliedTotal shouldBe 1L
  }

  test("RebalanceClosesTotal increments") {
    metrics.getRebalanceClosesTotal shouldBe 0L
    metrics.incrementRebalanceClosesTotal()
    metrics.incrementRebalanceClosesTotal()
    metrics.getRebalanceClosesTotal shouldBe 2L
  }

  // =========================================================================
  // F. State gauges
  // =========================================================================

  test("InFlightUploads increments and decrements") {
    metrics.getInFlightUploads shouldBe 0
    metrics.incrementInFlightUploads()
    metrics.incrementInFlightUploads()
    metrics.getInFlightUploads shouldBe 2
    metrics.decrementInFlightUploads()
    metrics.getInFlightUploads shouldBe 1
  }

  test("OldestOpenFileAgeMillis returns 0 when not set") {
    metrics.getOldestOpenFileAgeMillis shouldBe 0L
  }

  test("OldestOpenFileAgeMillis returns positive value when oldest timestamp is set") {
    metrics.setOldestOpenFileCreatedEpochMillis(System.currentTimeMillis() - 5000L)
    metrics.getOldestOpenFileAgeMillis should be > 0L
  }

  test("OldestOpenFileAgeMillis returns 0 after oldest timestamp is cleared") {
    metrics.setOldestOpenFileCreatedEpochMillis(System.currentTimeMillis() - 5000L)
    metrics.setOldestOpenFileCreatedEpochMillis(0L)
    metrics.getOldestOpenFileAgeMillis shouldBe 0L
  }

  // =========================================================================
  // JMX attribute exposure for new attributes
  // =========================================================================

  test("new attributes are accessible via JMX after registration") {
    val taskId = ConnectorTaskId("new-attrs-connector", 3, 0)
    val name   = new ObjectName("io.lenses.streamreactor.connect.cloud.sink:type=metrics,name=new-attrs-connector,task=0")
    val mbs    = ManagementFactory.getPlatformMBeanServer

    try {
      CloudSinkMetricsRegistrar.register(metrics, taskId)

      metrics.incrementFilesCommittedTotal()
      mbs.getAttribute(name, "FilesCommittedTotal") shouldBe 1L

      metrics.incrementFilesOpenedTotal()
      mbs.getAttribute(name, "FilesOpenedTotal") shouldBe 1L

      metrics.incrementSinkErrorsFatalTotal()
      mbs.getAttribute(name, "SinkErrorsFatalTotal") shouldBe 1L

      metrics.recordStorageUpload(50L, isError = false)
      mbs.getAttribute(name, "StorageUploadTimerCount") shouldBe 1L
      mbs.getAttribute(name, "StorageUploadTimerMaxMillis") shouldBe 50L
      mbs.getAttribute(name, "StorageUploadErrorsTotal") shouldBe 0L

      metrics.incrementRebalanceClosesTotal()
      mbs.getAttribute(name, "RebalanceClosesTotal") shouldBe 1L

      mbs.getAttribute(name, "InFlightUploads") shouldBe 0
    } finally {
      CloudSinkMetricsRegistrar.unregister(taskId)
    }
  }
}
