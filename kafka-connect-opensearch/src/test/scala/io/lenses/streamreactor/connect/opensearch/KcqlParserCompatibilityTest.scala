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
package io.lenses.streamreactor.connect.opensearch

import io.lenses.kcql.Kcql
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * KCQL parser-level compatibility tests for the OpenSearch connector.
 *
 * Behaviour tests for JsonBulkWriter itself (batching, IGNORE fields, tombstones, etc.)
 * live in elastic-common/JsonBulkWriterTest and are not duplicated here.
 */
class KcqlParserCompatibilityTest extends AnyFunSuite with Matchers {

  test("WITHDOCTYPE placed between INTO and SELECT is rejected by the KCQL parser") {
    intercept[Exception] {
      Kcql.parse("INSERT INTO orders WITHDOCTYPE _doc SELECT * FROM topic")
    }
  }
}
