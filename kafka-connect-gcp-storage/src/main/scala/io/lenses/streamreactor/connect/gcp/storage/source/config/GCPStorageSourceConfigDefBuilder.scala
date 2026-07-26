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
package io.lenses.streamreactor.connect.gcp.storage.source.config

import io.lenses.streamreactor.connect.cloud.common.source.config.CloudSourceConfigDefBuilder
import io.lenses.streamreactor.connect.gcp.storage.config.GCPSettings
import io.lenses.streamreactor.connect.gcp.storage.config.GCPConfigSettings

import scala.jdk.CollectionConverters.MapHasAsScala

case class GCPStorageSourceConfigDefBuilder(props: Map[String, AnyRef])
    extends CloudSourceConfigDefBuilder(GCPConfigSettings.CONNECTOR_PREFIX, GCPStorageSourceConfigDef.config, props)
    with GCPSettings {

  def getParsedValues: Map[String, _] = values().asScala.toMap

  // The GCP Storage lister searched from the directory the prefix names, whatever the spelling, and recursed exactly as
  // many levels as configured, so a recurse level of N found directories N levels below it.
  override protected def legacyRecurseLevels(levels: Int): (Int, Boolean) = (levels max 0, false)

}
