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

import scala.util.matching.Regex

object TemplateFileNamer {

  private val PlaceholderPattern: Regex = "\\{([^}]+)\\}".r

  private val KnownPlaceholders = Set(
    "topic",
    "partition",
    "start-offset",
    "end-offset",
    "start-timestamp",
    "end-timestamp",
    "record-count",
    "extension",
  )

  private val FlushVaryingPlaceholders = Set(
    "start-offset",
    "end-offset",
    "start-timestamp",
    "end-timestamp",
    "record-count",
  )

  /**
   * Validates the template at configuration time so that unknown placeholders are rejected before
   * the connector starts accepting records, and that at least one flush-varying placeholder is
   * present to prevent identical filenames (and silent overwrites) across consecutive flushes.
   */
  def validate(template: String): Either[Throwable, Unit] =
    if (template.trim.isEmpty) {
      Left(new IllegalArgumentException("object.key.template must not be blank"))
    } else {
      val found   = PlaceholderPattern.findAllMatchIn(template).map(_.group(1)).toList
      val unknown = found.filterNot(KnownPlaceholders)
      if (unknown.nonEmpty)
        Left(
          new IllegalArgumentException(
            s"object.key.template contains unknown placeholder(s): ${unknown.mkString(", ")}. " +
              s"Known placeholders are: ${KnownPlaceholders.toList.sorted.mkString(", ")}",
          ),
        )
      else if (found.toSet.intersect(FlushVaryingPlaceholders).isEmpty)
        Left(
          new IllegalArgumentException(
            "object.key.template must contain at least one flush-varying placeholder " +
              s"(${FlushVaryingPlaceholders.toList.sorted.mkString(", ")}) " +
              "to avoid overwriting files on consecutive flushes",
          ),
        )
      else
        Right(())
    }

  def apply(template: String, fileNamerConfig: FileNamerConfig): Either[Throwable, TemplateFileNamer] =
    validate(template).map(_ => new TemplateFileNamer(template, fileNamerConfig))
}

/**
 * A [[FileNamer]] that produces filenames by substituting named placeholders in a user-supplied template.
 *
 * Supported placeholders:
 *  - `{topic}`           — Kafka topic name (plain string)
 *  - `{partition}`       — partition number, padded via [[FileNamerConfig.partitionPaddingStrategy]]
 *  - `{start-offset}`    — first offset in the file, padded via [[FileNamerConfig.offsetPaddingStrategy]]
 *  - `{end-offset}`      — last offset in the file, padded via [[FileNamerConfig.offsetPaddingStrategy]]
 *  - `{start-timestamp}` — earliest record epoch ms (plain Long)
 *  - `{end-timestamp}`   — latest record epoch ms (plain Long)
 *  - `{record-count}`    — number of records in the file (plain Long)
 *  - `{extension}`       — format file extension (e.g. `parquet`, `avro`, `json`)
 *
 * Padding for `{partition}`, `{start-offset}`, and `{end-offset}` respects the KCQL
 * `PROPERTIES('padding.length'=..., 'padding.char'=..., 'padding.type'=...)` settings
 * via [[FileNamerConfig]], consistent with the built-in namers.
 *
 * Construct via [[TemplateFileNamer.apply]] to get upfront validation of unknown placeholders.
 */
class TemplateFileNamer(
  template:        String,
  fileNamerConfig: FileNamerConfig,
) extends FileNamer {

  override def fileName(params: FileNamerParams): String = {
    val topic       = params.topicPartitionOffset.topic.value
    val partition   = fileNamerConfig.partitionPaddingStrategy.padString(params.topicPartitionOffset.partition.toString)
    val startOffset = fileNamerConfig.offsetPaddingStrategy.padString(params.firstOffset.value.toString)
    val endOffset   = fileNamerConfig.offsetPaddingStrategy.padString(params.topicPartitionOffset.offset.value.toString)

    template
      .replace("{topic}", topic)
      .replace("{partition}", partition)
      .replace("{start-offset}", startOffset)
      .replace("{end-offset}", endOffset)
      .replace("{start-timestamp}", params.earliestRecordTimestamp.toString)
      .replace("{end-timestamp}", params.latestRecordTimestamp.toString)
      .replace("{record-count}", params.recordCount.toString)
      .replace("{extension}", fileNamerConfig.extension)
  }
}
