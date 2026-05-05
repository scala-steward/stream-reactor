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
package io.lenses.streamreactor.connect.datalake.sink

import cats.data.NonEmptyList
import io.lenses.streamreactor.common.config.base.const.TraitConfigConst.ERROR_POLICY_PROP_SUFFIX
import io.lenses.streamreactor.common.config.base.const.TraitConfigConst.MAX_RETRIES_PROP_SUFFIX
import io.lenses.streamreactor.common.config.base.const.TraitConfigConst.RETRY_INTERVAL_PROP_SUFFIX
import io.lenses.streamreactor.common.errors.FatalConnectException
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.FlushCount
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.sink.seek.CopyOperation
import io.lenses.streamreactor.connect.cloud.common.sink.seek.DeleteOperation
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexFile
import io.lenses.streamreactor.connect.cloud.common.sink.seek.ObjectWithETag
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingState
import io.lenses.streamreactor.connect.cloud.common.utils.ITSampleSchemaAndData._
import io.lenses.streamreactor.connect.datalake.utils.AzuriteContainerTest
import org.apache.commons.io.FileUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.errors.RetriableException
import org.apache.kafka.connect.sink.SinkTaskContext
import org.mockito.MockitoSugar
import org.scalatest.DoNotDiscover
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.SeqHasAsJava

/**
 * Azure-specific IT replay scenarios.
 *
 * These three scenarios exercise ADLS-specific behavior on top of the inherited
 * `CoreSinkTaskTestCases` matrix (run via [[AzureDatalakeSinkTaskTest]]):
 *
 * 1. crash-after-Copy idempotence: the ADLS `mvFile` 404-narrowing path (source absent,
 *    dest present → Right) is exercised end-to-end via the sink task restart path.
 *
 * 2. ADLS rename 404 explicit path: same scenario but we assert the recovery succeeds
 *    without a fatal error when the granular lock has PendingState=[Copy, Delete] and the
 *    source temp blob does not exist.
 *
 * 3. Fail fast when staging directory is deleted mid-commit (Azure parity with S3/GCS).
 */
@DoNotDiscover
class AzureDatalakeReplayScenariosTest
    extends AnyFlatSpec
    with AzuriteContainerTest
    with Matchers
    with MockitoSugar
    with EitherValues {

  private val context: SinkTaskContext = mock[SinkTaskContext]

  private val TopicName  = "myTopic"
  private val PrefixName = "streamReactorBackups"

  private val sinkRecords = firstUsers.zipWithIndex.map {
    case (user, k) =>
      new org.apache.kafka.connect.sink.SinkRecord(
        TopicName,
        1,
        null,
        null,
        schema,
        user,
        k.toLong,
        k.toLong,
        org.apache.kafka.common.record.TimestampType.CREATE_TIME,
      )
  }

  // ─── Scenario 1 ──────────────────────────────────────────────────────────────
  /**
   * crash-after-Copy idempotence: restart with PendingState=[Copy,Delete] where the ADLS source
   * blob is absent and the destination blob is present.  The ADLS `DatalakeStorageInterface.mvFile`
   * 404-narrowing returns Right (idempotent success), so the task must NOT throw
   * FatalConnectException.
   */
  "AzureDatalakeReplayScenariosTest" should "ADLS crash-after-Copy: replay with source missing and dest present completes idempotently (exercises ADLS rename-404 narrowing)" in {
    import IndexFile._

    val kcql =
      s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY name PROPERTIES('${FlushCount.entryName}'=1,'padding.length.partition'='12','padding.length.offset'='12')"
    val props = (defaultProps + (s"$prefix.kcql" -> kcql)).asJava

    val task1 = createSinkTask()
    task1.initialize(context)
    task1.start(props)
    task1.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task1.put(sinkRecords.slice(0, 1).asJava)
    task1.stop()

    val allFiles = listBucketPath(BucketName, "")

    val dataFilePaths = allFiles.filter(p => p.startsWith(PrefixName) && !p.contains(".indexes"))
    dataFilePaths should have size 1
    val finalPath = dataFilePaths.head

    val masterLockSuffix = "/1.lock"
    val granularLockPaths =
      allFiles.filter(p => p.contains("/.locks/") && p.endsWith(".lock") && !p.endsWith(masterLockSuffix))
    granularLockPaths should have size 1
    val granularLockPath = granularLockPaths.head

    val ObjectWithETag(currentLock, currentETag) =
      storageInterface.getBlobAsObject[IndexFile](BucketName, granularLockPath).value
    currentLock.pendingState shouldBe None

    val fakeTempPath = s".temp-upload/$TopicName/1/adls-replay-uuid"
    val pendingState = PendingState(
      currentLock.committedOffset.getOrElse(Offset(0L)),
      NonEmptyList.of(
        CopyOperation(BucketName, fakeTempPath, finalPath, currentETag),
        DeleteOperation(BucketName, fakeTempPath, currentETag),
      ),
    )
    storageInterface.writeBlobToFile(
      BucketName,
      granularLockPath,
      ObjectWithETag(currentLock.copy(pendingState = Some(pendingState)), currentETag),
    ).value

    val task2 = createSinkTask()
    task2.initialize(context)
    task2.start(props)
    task2.open(Seq(new TopicPartition(TopicName, 1)).asJava)

    noException should be thrownBy task2.put(sinkRecords.slice(0, 1).asJava)
    task2.stop()

    listBucketPath(BucketName, PrefixName + "/").size should be(1)
    listBucketPath(BucketName, ".temp-upload/").size should be(0)
  }

  // ─── Scenario 2 ──────────────────────────────────────────────────────────────
  /**
   * Same as Scenario 1 but using a different fake-temp path to confirm the 404 path is
   * stable across multiple recovery cycles in a single test run.
   */
  it should "ADLS rename returns 404 source-missing/dest-present during a replay — sink task does NOT throw fatal; completes the chain" in {
    import IndexFile._

    val kcql =
      s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY name PROPERTIES('${FlushCount.entryName}'=1,'padding.length.partition'='12','padding.length.offset'='12')"
    val props = (defaultProps + (s"$prefix.kcql" -> kcql)).asJava

    val task1 = createSinkTask()
    task1.initialize(context)
    task1.start(props)
    task1.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task1.put(sinkRecords.slice(0, 1).asJava)
    task1.stop()

    val allFiles      = listBucketPath(BucketName, "")
    val dataFilePaths = allFiles.filter(p => p.startsWith(PrefixName) && !p.contains(".indexes"))
    dataFilePaths should have size 1
    val finalPath = dataFilePaths.head

    val masterLockSuffix = "/1.lock"
    val granularLockPaths =
      allFiles.filter(p => p.contains("/.locks/") && p.endsWith(".lock") && !p.endsWith(masterLockSuffix))
    granularLockPaths should have size 1
    val granularLockPath = granularLockPaths.head

    val ObjectWithETag(currentLock, currentETag) =
      storageInterface.getBlobAsObject[IndexFile](BucketName, granularLockPath).value

    // Non-existent source → the ADLS mvFile call returns 404 (source absent) and dest present → Right.
    val nonExistentSource = s".temp-upload/$TopicName/1/never-uploaded-uuid"
    val pendingState = PendingState(
      currentLock.committedOffset.getOrElse(Offset(0L)),
      NonEmptyList.of(
        CopyOperation(BucketName, nonExistentSource, finalPath, currentETag),
        DeleteOperation(BucketName, nonExistentSource, currentETag),
      ),
    )
    storageInterface.writeBlobToFile(
      BucketName,
      granularLockPath,
      ObjectWithETag(currentLock.copy(pendingState = Some(pendingState)), currentETag),
    ).value

    val task2 = createSinkTask()
    task2.initialize(context)
    task2.start(props)
    task2.open(Seq(new TopicPartition(TopicName, 1)).asJava)

    noException should be thrownBy task2.put(sinkRecords.slice(0, 1).asJava)
    task2.stop()

    listBucketPath(BucketName, PrefixName + "/").size should be(1)
  }

  // ─── Scenario 3 ──────────────────────────────────────────────────────────────
  /**
   * Azure parity for the "fail fast when staging directory is deleted mid-commit" scenario
   * that exists in S3 and GCP IT tests.
   */
  it should "fail fast when staging directory is deleted mid-commit (Azure parity)" in {

    val task = createSinkTask()
    task.initialize(context)

    val tmpDir = Files.createTempDirectory("azureTestTempDir").toFile

    val props = (defaultProps ++
      Map(
        s"$prefix.kcql"                          -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `json` PROPERTIES('${FlushCount.entryName}'=1,'padding.length.partition'='12', 'padding.length.offset'='12')",
        s"$prefix.$ERROR_POLICY_PROP_SUFFIX"     -> "RETRY",
        s"$prefix.$RETRY_INTERVAL_PROP_SUFFIX"   -> "10",
        s"$prefix.http.$MAX_RETRIES_PROP_SUFFIX" -> "5",
        s"$prefix.local.tmp.directory"           -> tmpDir.getAbsolutePath,
        s"$prefix.http.socket.timeout"           -> "200",
        s"$prefix.http.connection.timeout"       -> "200",
      )).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)

    // Record 0 commits cleanly.
    task.put(sinkRecords.slice(0, 1).asJava)

    // Pause Azurite to simulate a transient cloud outage; next put() leaves writer in Uploading.
    container.pause()
    intercept[RetriableException] {
      task.put(sinkRecords.slice(1, 2).asJava)
    }

    // Simulate staging directory being wiped (tmpwatch / container RuntimeDirectory clean-up).
    FileUtils.deleteDirectory(tmpDir)
    tmpDir.exists() should be(false)

    container.resume()

    // recommitPending detects the missing staging file → FatalCloudSinkError → FatalConnectException.
    val thrown = intercept[FatalConnectException] {
      task.put(sinkRecords.slice(0, 3).asJava)
    }
    thrown.getMessage should include("Local staging file disappeared mid-commit")
    thrown.getMessage should include(s"$TopicName-1")
    Option(thrown.getCause) match {
      case Some(_: IllegalStateException) => succeed
      case other => fail(s"Expected cause to be IllegalStateException, was: $other")
    }

    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000001/").size should be(1)
    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/000000000001/000000000000_0_0.json") should be(
      """{"name":"sam","title":"mr","salary":100.43}""",
    )
  }
}
