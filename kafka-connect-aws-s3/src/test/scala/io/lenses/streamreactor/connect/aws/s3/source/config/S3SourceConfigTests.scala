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
package io.lenses.streamreactor.connect.aws.s3.source.config

import io.lenses.streamreactor.common.config.base.KcqlSettings
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings._
import io.lenses.streamreactor.connect.aws.s3.model.location.S3LocationValidator
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.config.TaskIndexKey
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.source.config.PartitionSearcherOptions
import io.lenses.streamreactor.connect.cloud.common.source.config.CloudSourceSettingsKeys
import io.lenses.streamreactor.connect.cloud.common.source.config.PartitionSearcherOptions.ExcludeIndexes
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class S3SourceConfigTests
    extends AnyFunSuite
    with Matchers
    with EitherValues
    with TaskIndexKey
    with CloudSourceSettingsKeys {

  implicit val taskId:    ConnectorTaskId        = ConnectorTaskId("test", 1, 1)
  implicit val validator: CloudLocationValidator = S3LocationValidator

  val KCQL_CONFIG = new KcqlSettings(javaConnectorPrefix).getKcqlSettingsKey

  private def parse(props: Seq[(String, String)]): Either[Throwable, S3SourceConfig] =
    S3SourceConfig.fromProps(
      taskId,
      Map(
        TASK_INDEX  -> "1:0",
        KCQL_CONFIG -> "INSERT INTO topic SELECT * FROM bucket:/a/b/c",
      ) ++ props,
    )

  private def configFrom(props: (String, String)*): PartitionSearcherOptions =
    parse(props) match {
      case Left(value)  => fail(value.toString)
      case Right(value) => value.partitionSearcher
    }

  private def failureFrom(props: (String, String)*): Throwable = parse(props).left.value

  /** The deprecated key is the only source of the depth in these cases, so the search starts at the prefix as configured. */
  private def legacyOptions(partitionDepth: Int, continuous: Boolean): PartitionSearcherOptions =
    PartitionSearcherOptions(
      partitionDepth     = partitionDepth,
      prefixAsConfigured = true,
      continuous         = continuous,
      interval           = 1.seconds,
      wildcardExcludes   = ExcludeIndexes,
    )

  test("neither depth nor the deprecated recurse levels set searches one level from the prefix as configured") {
    configFrom(
      SOURCE_PARTITION_SEARCH_MODE            -> "false",
      SOURCE_PARTITION_SEARCH_INTERVAL_MILLIS -> "1000",
    ) shouldBe legacyOptions(partitionDepth = 1, continuous = false)
  }
  test("not specifying the SOURCE_PARTITION_SEARCH_MODE defaults to true") {
    configFrom(
      SOURCE_PARTITION_SEARCH_RECURSE_LEVELS  -> "1",
      SOURCE_PARTITION_SEARCH_INTERVAL_MILLIS -> "1000",
    ) shouldBe legacyOptions(partitionDepth = 2, continuous = true)
  }
  // The old S3 lister counted its first level from the prefix as configured, so the depth is one greater and the search
  // has to start there for a prefix without a trailing slash to behave as it did.
  test("the deprecated recurse levels searches one level deeper from the prefix as configured") {
    configFrom(SOURCE_PARTITION_SEARCH_RECURSE_LEVELS -> "0").partitionDepth shouldBe 1
    configFrom(SOURCE_PARTITION_SEARCH_RECURSE_LEVELS -> "2").partitionDepth shouldBe 3
    configFrom(SOURCE_PARTITION_SEARCH_RECURSE_LEVELS -> "1").prefixAsConfigured shouldBe true
  }
  test("the partition search depth is taken as given and searches from the directory the prefix names") {
    configFrom(SOURCE_PARTITION_SEARCH_DEPTH -> "0").partitionDepth shouldBe 0
    configFrom(SOURCE_PARTITION_SEARCH_DEPTH -> "2").partitionDepth shouldBe 2
    configFrom(SOURCE_PARTITION_SEARCH_DEPTH -> "2").prefixAsConfigured shouldBe false
  }
  test("setting both the partition search depth and the deprecated recurse levels is rejected") {
    failureFrom(
      SOURCE_PARTITION_SEARCH_DEPTH          -> "2",
      SOURCE_PARTITION_SEARCH_RECURSE_LEVELS -> "1",
    ).getMessage should include(SOURCE_PARTITION_SEARCH_RECURSE_LEVELS)
  }
  test("a negative partition search depth is rejected rather than treated as unset") {
    failureFrom(SOURCE_PARTITION_SEARCH_DEPTH -> "-1").getMessage should include(SOURCE_PARTITION_SEARCH_DEPTH)
  }

  override def connectorPrefix: String = CONNECTOR_PREFIX
}
