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
package io.lenses.streamreactor.connect.gcp.storage.config

import io.lenses.streamreactor.common.config.base.traits.BaseSettings
import io.lenses.streamreactor.common.config.base.traits.WithConnectorPrefix
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type
trait UploadConfigKeys extends WithConnectorPrefix {

  /**
   * Deprecated. Uploads are now always streamed via resumable sessions using `storage.createFrom`, which keeps heap
   * usage bounded regardless of file size. This setting is accepted for backwards compatibility but has no effect.
   */
  protected val AVOID_RESUMABLE_UPLOAD: String = s"$connectorPrefix.avoid.resumable.upload"

  def addUploadSettingsToConfigDef(configDef: ConfigDef): ConfigDef =
    configDef.define(
      AVOID_RESUMABLE_UPLOAD,
      Type.BOOLEAN,
      "false",
      Importance.LOW,
      "[Deprecated, ignored] Previously used to switch from resumable to single-request uploads. Uploads are now always streamed via resumable sessions and this setting has no effect.",
    )
}
trait UploadSettings extends BaseSettings with UploadConfigKeys
