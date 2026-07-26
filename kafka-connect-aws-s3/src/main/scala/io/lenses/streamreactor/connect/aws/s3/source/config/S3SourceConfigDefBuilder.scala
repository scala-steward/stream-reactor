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

import io.lenses.streamreactor.connect.aws.s3.config.DeleteModeSettings
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings
import io.lenses.streamreactor.connect.cloud.common.source.config.CloudSourceConfigDefBuilder

import scala.jdk.CollectionConverters.MapHasAsScala

case class S3SourceConfigDefBuilder(props: Map[String, AnyRef])
    extends CloudSourceConfigDefBuilder(S3ConfigSettings.CONNECTOR_PREFIX, S3SourceConfigDef.config, props)
    with DeleteModeSettings {

  def getParsedValues: Map[String, _] = values().asScala.toMap

  // The S3 lister searched from the prefix exactly as configured and counted that as its first level, so a recurse level
  // of N searched N + 1 levels from the prefix as written.  For a prefix ending in `/` that is N + 1 levels below the
  // directory, but for one without a trailing slash the first level is spent reaching the directory, and sibling
  // directories sharing the prefix are matched too.  See LC-316.
  override protected def legacyRecurseLevels(levels: Int): (Int, Boolean) = ((levels max 0) + 1, true)

}
