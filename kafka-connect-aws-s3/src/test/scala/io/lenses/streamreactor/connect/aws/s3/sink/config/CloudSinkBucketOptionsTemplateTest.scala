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
package io.lenses.streamreactor.connect.aws.s3.sink.config

import io.lenses.streamreactor.connect.aws.s3.model.location.S3LocationValidator
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.FlushCount
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartitionOffset
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.sink.config.CloudSinkBucketOptions
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionPartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.config.TopicPartitionField
import org.scalatest.EitherValues
import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests that `CloudSinkBucketOptions.getFileNamer` correctly selects `TemplateFileNamer` when
 * `object.key.template` is present in KCQL PROPERTIES, and falls back to the built-in namers
 * when absent.
 *
 * The full cloud object key is {partitionPrefix}/{fileNamer.fileName}. These tests check the
 * filename portion (after the last '/') to distinguish FileNamer implementations.
 */
class CloudSinkBucketOptionsTemplateTest extends AnyFunSuite with Matchers with EitherValues with OptionValues {

  private implicit val cloudLocationValidator: CloudLocationValidator = S3LocationValidator
  private implicit val connectorTaskId:        ConnectorTaskId        = ConnectorTaskId("test-connector", 1, 0)

  private val tpoA = Topic("topic-a").withPartition(0).atOffset(100L)

  private def defaultPartitionValues(tpo: TopicPartitionOffset): Map[PartitionField, String] =
    Map[PartitionField, String](
      TopicPartitionField     -> tpo.topic.value,
      PartitionPartitionField -> tpo.partition.toString,
    )

  private def fileName(
    kcql:     String,
    tpo:      TopicPartitionOffset,
    firstOff: Offset = Offset(50L),
    recCount: Long   = 3L,
  ): String = {
    val opts = CloudSinkBucketOptions(connectorTaskId, S3SinkConfigDefBuilder(Map("connect.s3.kcql" -> kcql))).value
    opts should have size 1
    val opt = opts.head
    val path = opt.keyNamer.value(opt.bucketAndPrefix,
                                  tpo,
                                  defaultPartitionValues(tpo),
                                  firstOff,
                                  1000L,
                                  2000L,
                                  recCount,
    ).value.path.value
    // Return just the filename portion after the last '/'
    path.split('/').last
  }

  test("OffsetFileNamer is used when object.key.template is absent: filename is offset-based") {
    val name = fileName(
      s"INSERT INTO my-bucket SELECT * FROM topic-a PROPERTIES('${FlushCount.entryName}'=1)",
      tpoA,
    )
    // OffsetFileNamer: <paddedOffset>_<earliestTs>_<latestTs>.<ext>
    name shouldEqual "000000000100_1000_2000.json"
  }

  test("TemplateFileNamer: {topic} in template produces topic in filename") {
    val name = fileName(
      s"INSERT INTO my-bucket SELECT * FROM topic-a PROPERTIES('${FlushCount.entryName}'=1,'object.key.template'='{topic}-{record-count}.{extension}')",
      tpoA,
    )
    name shouldEqual "topic-a-3.json"
  }

  test("TemplateFileNamer: {record-count} is rendered as plain number in filename") {
    val name = fileName(
      s"INSERT INTO my-bucket SELECT * FROM topic-a PROPERTIES('object.key.template'='{record-count}.{extension}')",
      tpoA,
      recCount = 7L,
    )
    name shouldEqual "7.json"
  }

  test("TemplateFileNamer: {start-offset} and {end-offset} are independently rendered with padding") {
    val name = fileName(
      s"INSERT INTO my-bucket SELECT * FROM topic-a PROPERTIES('object.key.template'='{start-offset}-{end-offset}')",
      tpoA,
      firstOff = Offset(5L),
    )
    // Default S3 padding: LeftPad 12 zeros
    name shouldEqual "000000000005-000000000100"
  }

  test("TemplateFileNamer: {start-timestamp} and {end-timestamp} are plain longs") {
    val name = fileName(
      s"INSERT INTO my-bucket SELECT * FROM topic-a PROPERTIES('object.key.template'='{start-timestamp}_{end-timestamp}')",
      tpoA,
    )
    name shouldEqual "1000_2000"
  }

  test("TemplateFileNamer: {partition} uses default padding strategy (no padding for partition by default)") {
    val tpo = Topic("topic-a").withPartition(3).atOffset(1L)
    val name = fileName(
      s"INSERT INTO my-bucket SELECT * FROM topic-a PROPERTIES('object.key.template'='{partition}-{record-count}')",
      tpo,
    )
    // Default S3 partition padding: NoOp (no padding)
    name shouldEqual "3-3"
  }

  test("TemplateFileNamer: composite template with multiple placeholders") {
    val name = fileName(
      s"INSERT INTO my-bucket SELECT * FROM topic-a PROPERTIES('object.key.template'='{topic}_{record-count}.{extension}')",
      tpoA,
      recCount = 5L,
    )
    name shouldEqual "topic-a_5.json"
  }

  test("TemplateFileNamer: literal extension in template is emitted verbatim — {extension} is optional") {
    val name = fileName(
      s"INSERT INTO my-bucket SELECT * FROM topic-a PROPERTIES('object.key.template'='{topic}-{record-count}.xyz')",
      tpoA,
      recCount = 5L,
    )
    name shouldEqual "topic-a-5.xyz"
  }

  test("invalid template placeholder is rejected at construction time") {
    val result = CloudSinkBucketOptions(
      connectorTaskId,
      S3SinkConfigDefBuilder(
        Map(
          "connect.s3.kcql" -> "INSERT INTO my-bucket SELECT * FROM topic-a PROPERTIES('object.key.template'='{bad-placeholder}')",
        ),
      ),
    )
    result.isLeft shouldBe true
    result.left.value.getMessage should include("bad-placeholder")
  }

  test("blank template is rejected at construction time") {
    val result = CloudSinkBucketOptions(
      connectorTaskId,
      S3SinkConfigDefBuilder(
        Map("connect.s3.kcql" -> "INSERT INTO my-bucket SELECT * FROM topic-a PROPERTIES('object.key.template'=' ')"),
      ),
    )
    result.isLeft shouldBe true
    result.left.value.getMessage should include("blank")
  }

  test("static-only template is rejected at construction time") {
    val result = CloudSinkBucketOptions(
      connectorTaskId,
      S3SinkConfigDefBuilder(
        Map(
          "connect.s3.kcql" -> "INSERT INTO my-bucket SELECT * FROM topic-a PROPERTIES('object.key.template'='{topic}.{extension}')",
        ),
      ),
    )
    result.isLeft shouldBe true
    result.left.value.getMessage should include("flush-varying")
  }

  test("per-KCQL isolation: statement A uses template, statement B uses built-in namer") {
    val kcqls =
      s"INSERT INTO bucket-a SELECT * FROM topic-a PROPERTIES('object.key.template'='{topic}-{record-count}.{extension}');" +
        s"INSERT INTO bucket-b SELECT * FROM topic-b PROPERTIES('${FlushCount.entryName}'=10)"

    val opts = CloudSinkBucketOptions(connectorTaskId, S3SinkConfigDefBuilder(Map("connect.s3.kcql" -> kcqls))).value
    opts should have size 2

    val optA = opts.find(_.sourceTopic.contains("topic-a")).value
    val optB = opts.find(_.sourceTopic.contains("topic-b")).value

    val tpoB = Topic("topic-b").withPartition(0).atOffset(200L)
    val pathA = optA.keyNamer.value(optA.bucketAndPrefix,
                                    tpoA,
                                    defaultPartitionValues(tpoA),
                                    tpoA.offset,
                                    0L,
                                    0L,
                                    1L,
    ).value.path.value
    val pathB = optB.keyNamer.value(optB.bucketAndPrefix,
                                    tpoB,
                                    defaultPartitionValues(tpoB),
                                    tpoB.offset,
                                    0L,
                                    0L,
                                    1L,
    ).value.path.value
    val nameA = pathA.split('/').last
    val nameB = pathB.split('/').last

    // A: TemplateFileNamer → filename is topic-a-1.json (record-count=1L)
    nameA shouldEqual "topic-a-1.json"
    // B: OffsetFileNamer → filename is offset-based, not topic name
    nameB should not include "topic-b"
    nameB should endWith(".json")
  }
}
