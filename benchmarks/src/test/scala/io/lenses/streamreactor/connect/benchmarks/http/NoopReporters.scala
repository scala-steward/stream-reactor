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
package io.lenses.streamreactor.connect.benchmarks.http

import io.lenses.streamreactor.connect.http.sink.reporter.converter.HttpFailureSpecificHeaderRecordConverter
import io.lenses.streamreactor.connect.http.sink.reporter.converter.HttpSuccessSpecificHeaderRecordConverter
import io.lenses.streamreactor.connect.http.sink.reporter.model.HttpFailureConnectorSpecificRecordData
import io.lenses.streamreactor.connect.http.sink.reporter.model.HttpSuccessConnectorSpecificRecordData
import io.lenses.streamreactor.connect.reporting.ReportingController
import io.lenses.streamreactor.connect.reporting.ReportingMessagesConfig
import io.lenses.streamreactor.connect.reporting.model.RecordConverter

import java.util
import scala.jdk.CollectionConverters.MapHasAsJava

/**
 * Disabled (no-op) success/error reporting controllers, built the same way `HttpSinkConfig.from`
 * builds them in production -- an empty sender config leaves `reporting.enabled` at its default
 * of `false`, so `enqueue`/`start`/`close` are all no-ops. Kept separate from the harness so the
 * benchmark never has to configure a real Kafka reporting topic.
 */
object NoopReporters {

  private val disabledConfig: util.Map[String, AnyRef] = Map.empty[String, AnyRef].asJava

  val error: ReportingController[HttpFailureConnectorSpecificRecordData] =
    ReportingController.fromConfig[HttpFailureConnectorSpecificRecordData](
      (cfg: ReportingMessagesConfig) => new RecordConverter(cfg, HttpFailureSpecificHeaderRecordConverter.apply),
      disabledConfig,
    )

  val success: ReportingController[HttpSuccessConnectorSpecificRecordData] =
    ReportingController.fromConfig[HttpSuccessConnectorSpecificRecordData](
      (cfg: ReportingMessagesConfig) => new RecordConverter(cfg, HttpSuccessSpecificHeaderRecordConverter.apply),
      disabledConfig,
    )
}
