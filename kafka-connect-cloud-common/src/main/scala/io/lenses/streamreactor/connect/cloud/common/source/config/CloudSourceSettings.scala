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
package io.lenses.streamreactor.connect.cloud.common.source.config

import cats.implicits.catsSyntaxEitherId
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.common.config.base.traits.BaseSettings
import io.lenses.streamreactor.connect.cloud.common.config.ConfigParse
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEntry
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum
import io.lenses.streamreactor.connect.cloud.common.storage.ExtensionFilter
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata
import io.lenses.streamreactor.connect.config.kcqlprops.KcqlProperties

import scala.concurrent.duration.DurationLong
import scala.util.Try

trait CloudSourceSettings extends BaseSettings with CloudSourceSettingsKeys with LazyLogging {

  def extractOrderingType[M <: FileMetadata]: OrderingType =
    Try(getString(SOURCE_ORDERING_TYPE)).toOption.flatMap(
      OrderingType.withNameInsensitiveOption,
    ).getOrElse(OrderingType.AlphaNumeric)

  def getPartitionExtractor(parsedValues: Map[String, _]): Option[PartitionExtractor] = PartitionExtractor(
    ConfigParse.getString(parsedValues, SOURCE_PARTITION_EXTRACTOR_TYPE).getOrElse("none"),
    ConfigParse.getString(parsedValues, SOURCE_PARTITION_EXTRACTOR_REGEX),
  )

  def extractEnvelope(
    properties: KcqlProperties[PropsKeyEntry, PropsKeyEnum.type],
  ): Either[Throwable, Option[Boolean]] =
    properties.getOptionalBoolean(PropsKeyEnum.StoreEnvelope)

  /**
   * The keys the user actually supplied, as opposed to those defaulted by the ConfigDef.  Needed to tell an explicit
   * value apart from a default when resolving the partition depth.
   */
  protected def explicitlySetKeys: Set[String]

  /**
   * Translates the deprecated `source.partition.search.recurse.levels` into a partition depth, and reports whether the
   * search starts from the prefix exactly as configured.  This is the only place the clouds differ: the S3 lister
   * searched from the prefix as given, the GCP Storage one searched from the directory that prefix names.
   */
  protected def legacyRecurseLevels(levels: Int): (Int, Boolean)

  def getPartitionSearcherOptions(props: Map[String, _]): Either[Throwable, PartitionSearcherOptions] =
    resolvePartitionDepth().map {
      case (depth, prefixAsConfigured) =>
        PartitionSearcherOptions(
          partitionDepth     = depth,
          prefixAsConfigured = prefixAsConfigured,
          continuous         = getBoolean(SOURCE_PARTITION_SEARCH_MODE),
          interval = ConfigParse.getLong(props, SOURCE_PARTITION_SEARCH_INTERVAL_MILLIS).getOrElse(
            SOURCE_PARTITION_SEARCH_INTERVAL_MILLIS_DEFAULT,
          ).millis,
          wildcardExcludes = getString(PARTITION_SEARCH_INDEX_EXCLUDES).split(',').toSet[String].map(_.trim),
        )
    }

  private def resolvePartitionDepth(): Either[Throwable, (Int, Boolean)] = {
    val depthSet  = explicitlySetKeys.contains(SOURCE_PARTITION_SEARCH_DEPTH)
    val legacySet = explicitlySetKeys.contains(SOURCE_PARTITION_SEARCH_RECURSE_LEVELS)

    if (depthSet && legacySet)
      new IllegalArgumentException(
        s"Both `$SOURCE_PARTITION_SEARCH_DEPTH` and the deprecated `$SOURCE_PARTITION_SEARCH_RECURSE_LEVELS` are set. Please remove `$SOURCE_PARTITION_SEARCH_RECURSE_LEVELS`.",
      ).asLeft
    else if (depthSet) {
      val depth = getInt(SOURCE_PARTITION_SEARCH_DEPTH).toInt
      if (depth < 0)
        new IllegalArgumentException(
          s"`$SOURCE_PARTITION_SEARCH_DEPTH` must be 0 or greater, but was $depth. 0 treats the configured prefix as the only partition directory, 1 its immediate children, and so on.",
        ).asLeft
      else (depth, false).asRight
    } else {
      val (depth, prefixAsConfigured) = legacyRecurseLevels(getInt(SOURCE_PARTITION_SEARCH_RECURSE_LEVELS))
      if (legacySet) {
        val equivalent =
          if (prefixAsConfigured)
            s"$depth below a prefix ending in `/`, or ${(depth - 1) max 0} below one without a trailing slash, which also stops matching sibling directories that share the prefix"
          else s"$depth"
        logger.warn(
          s"`$SOURCE_PARTITION_SEARCH_RECURSE_LEVELS` is deprecated because it does not mean the same depth on every cloud. Please use `$SOURCE_PARTITION_SEARCH_DEPTH`, which for this connector is equivalent to a depth of $equivalent.",
        )
      }
      (depth, prefixAsConfigured).asRight
    }
  }

  def getEmptySourceBackoffSettings(properties: Map[String, _]): EmptySourceBackoffSettings =
    EmptySourceBackoffSettings.apply(
      ConfigParse.getLong(properties, SOURCE_EMPTY_RESULTS_BACKOFF_INITIAL_DELAY).getOrElse(
        SOURCE_EMPTY_RESULTS_BACKOFF_INITIAL_DELAY_DEFAULT,
      ),
      ConfigParse.getLong(properties, SOURCE_EMPTY_RESULTS_BACKOFF_MAX_DELAY).getOrElse(
        SOURCE_EMPTY_RESULTS_BACKOFF_MAX_DELAY_DEFAULT,
      ),
      ConfigParse.getDouble(properties, SOURCE_EMPTY_RESULTS_BACKOFF_MULTIPLIER).getOrElse(
        SOURCE_EMPTY_RESULTS_BACKOFF_MULTIPLIER_DEFAULT,
      ),
    )

  /**
   * Retrieves the extension filter for the source.
   *
   * The extension filter is used to include or exclude files
   * based on their extensions when reading from the source.
   *
   * @return The extension filter for the source.
   */
  def getSourceExtensionFilter: Option[ExtensionFilter] = {

    val includes = extractSetFromProperty(SOURCE_EXTENSION_INCLUDES)
    val excludes = extractSetFromProperty(SOURCE_EXTENSION_EXCLUDES)
    Option.when(includes.nonEmpty || excludes.nonEmpty)(new ExtensionFilter(includes.getOrElse(Set.empty),
                                                                            excludes.getOrElse(Set.empty),
    ))
  }

  def getWriteWatermarkToHeaders: Boolean = getBoolean(WRITE_WATERMARK_TO_HEADERS)

  /**
   * Retrieves the late arrival interval in seconds.
   * This is used by the LateArrivalTouchTask to determine how often to check for late-arriving files.
   * Returns the default value if not configured or if the configured value is not positive.
   *
   * @return The late arrival interval in seconds.
   */
  def getLateArrivalInterval: Int = {
    val interval = getInt(SOURCE_LATE_ARRIVAL_INTERVAL)
    if (interval > 0) interval else SOURCE_LATE_ARRIVAL_INTERVAL_DEFAULT
  }

  /**
   * Extracts the property value from the configuration and transforms it into a set of strings.
   *
   * Each string in the set represents a file extension. If the extension does not start with a dot, one is added.
   *
   * @param propertyName The name of the property to extract.
   * @return An Option containing a set of strings if the property exists, None otherwise.
   */
  private def extractSetFromProperty(propertyName: String): Option[Set[String]] =
    Option(getString(propertyName)).map(_.split(",").map(_.toLowerCase).map(s =>
      if (s.startsWith(".")) s else s".$s",
    ).toSet)
}
