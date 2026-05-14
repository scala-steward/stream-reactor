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
package io.lenses.streamreactor.connect.cloud.common.sink.naming

import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.sink.config.padding.LeftPadPaddingStrategy
import io.lenses.streamreactor.connect.cloud.common.sink.config.padding.NoOpPaddingStrategy
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TemplateFileNamerTest extends AnyFunSuite with Matchers with EitherValues {

  private val paddingStrategy = LeftPadPaddingStrategy(5, '0')
  private val noPadding       = NoOpPaddingStrategy

  private val config = FileNamerConfig(
    partitionPaddingStrategy = paddingStrategy,
    offsetPaddingStrategy    = paddingStrategy,
    extension                = "parquet",
    suffix                   = None,
  )

  private val tpo = Topic("my-topic").withPartition(3).atOffset(150L)

  private def params(firstOffset: Long = 100L, recordCount: Long = 5L) =
    FileNamerParams(
      topicPartitionOffset    = tpo,
      earliestRecordTimestamp = 1000L,
      latestRecordTimestamp   = 2000L,
      firstOffset             = Offset(firstOffset),
      recordCount             = recordCount,
    )

  test("validation rejects blank template") {
    TemplateFileNamer.validate("").left.value.getMessage should include("must not be blank")
    TemplateFileNamer.validate("   ").left.value.getMessage should include("must not be blank")
  }

  test("validation rejects unknown placeholders") {
    val err = TemplateFileNamer.validate("{unknown}").left.value
    err.getMessage should include("unknown")
    err.getMessage should include("Known placeholders")
  }

  test("validation accepts all known placeholders") {
    val template = "{topic}/{partition}/{start-offset}-{end-offset}-{record-count}_{start-timestamp}_{end-timestamp}.{extension}"
    TemplateFileNamer.validate(template).value shouldBe ()
  }

  test("{topic} placeholder is substituted correctly") {
    val namer = TemplateFileNamer("{topic}.{extension}", config).value
    namer.fileName(params()) shouldEqual "my-topic.parquet"
  }

  test("{partition} placeholder uses partitionPaddingStrategy") {
    val namer = TemplateFileNamer("{partition}", config).value
    namer.fileName(params()) shouldEqual "00003"
  }

  test("{start-offset} placeholder uses offsetPaddingStrategy") {
    val namer = TemplateFileNamer("{start-offset}", config).value
    namer.fileName(params(firstOffset = 42L)) shouldEqual "00042"
  }

  test("{end-offset} placeholder uses offsetPaddingStrategy") {
    val namer = TemplateFileNamer("{end-offset}", config).value
    namer.fileName(params()) shouldEqual "00150"
  }

  test("{start-timestamp} placeholder is plain Long string (no padding)") {
    val namer = TemplateFileNamer("{start-timestamp}", config).value
    namer.fileName(params()) shouldEqual "1000"
  }

  test("{end-timestamp} placeholder is plain Long string (no padding)") {
    val namer = TemplateFileNamer("{end-timestamp}", config).value
    namer.fileName(params()) shouldEqual "2000"
  }

  test("{record-count} placeholder is plain Long string (no padding)") {
    val namer = TemplateFileNamer("{record-count}", config).value
    namer.fileName(params(recordCount = 42L)) shouldEqual "42"
  }

  test("{extension} placeholder uses fileNamerConfig.extension") {
    val namer = TemplateFileNamer("{extension}", config).value
    namer.fileName(params()) shouldEqual "parquet"
  }

  test("composite template produces correct filename") {
    val namer = TemplateFileNamer("{topic}/{partition}/{start-offset}-{end-offset}-{record-count}.{extension}", config).value
    namer.fileName(params(firstOffset = 100L, recordCount = 5L)) shouldEqual
      "my-topic/00003/00100-00150-5.parquet"
  }

  test("multiple occurrences of the same placeholder are all substituted") {
    val namer = TemplateFileNamer("{topic}-{topic}", config).value
    namer.fileName(params()) shouldEqual "my-topic-my-topic"
  }

  test("literal slashes in template are preserved (sub-directory paths)") {
    val namer = TemplateFileNamer("prefix/{topic}/data/{partition}", config).value
    namer.fileName(params()) shouldEqual "prefix/my-topic/data/00003"
  }

  test("padding does not apply to {record-count} or timestamps") {
    val configNoPadding = config.copy(
      partitionPaddingStrategy = noPadding,
      offsetPaddingStrategy    = noPadding,
    )
    val namer = TemplateFileNamer("{record-count}/{start-timestamp}/{end-timestamp}", configNoPadding).value
    namer.fileName(params(recordCount = 7L)) shouldEqual "7/1000/2000"
  }

  test("start-offset and end-offset can differ") {
    val namer = TemplateFileNamer("{start-offset}-{end-offset}", config).value
    namer.fileName(params(firstOffset = 10L)) shouldEqual "00010-00150"
  }
}
