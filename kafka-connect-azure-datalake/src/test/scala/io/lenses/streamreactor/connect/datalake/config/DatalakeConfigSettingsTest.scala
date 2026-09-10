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
package io.lenses.streamreactor.connect.datalake.config

import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.datalake.sink.config.DatalakeSinkConfigDef
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.CollectionHasAsScala

class DatalakeConfigSettingsTest extends AnyFlatSpec with Matchers with LazyLogging {

  "DatalakeConfigSettings" should "ensure all sink keys are lower case" in {
    val configKeys = DatalakeSinkConfigDef.config.configKeys().keySet().asScala
    configKeys.foreach(k => k.toLowerCase should be(k))
  }

  it should "expose the partition-batch commit mode key (T1.4)" in {
    val configKeys = DatalakeSinkConfigDef.config.configKeys().keySet().asScala
    configKeys should contain(s"${AzureConfigSettings.CONNECTOR_PREFIX}.exactly.once.commit.mode")
  }
}
