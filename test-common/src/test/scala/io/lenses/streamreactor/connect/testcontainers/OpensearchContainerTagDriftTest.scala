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
package io.lenses.streamreactor.connect.testcontainers

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Guards that `OpensearchContainer.defaultTag` matches the opensearch-java client version
 * defined in `project/Dependencies.scala`.
 *
 * The constant is inlined at compile time from Dependencies.scala via a build-generated
 * resource or via a hardcoded value that must be updated together with the dep.
 *
 * Drift causes the IT containers to run a different server version than the client is compiled against.
 */
class OpensearchContainerTagDriftTest extends AnyFunSuite with Matchers {

  // This value MUST match:
  //  1. Dependencies.openSearchVersion in project/Dependencies.scala
  //  2. OpensearchContainer.defaultTag in OpensearchContainer.scala
  //  3. The opensearch-java client version pinned in kafkaConnectOpenSearchDeps
  private val expectedVersion = "2.13.0"

  test("OpensearchContainer.defaultTag matches the expected opensearch version") {
    OpensearchContainer.defaultTag shouldBe expectedVersion
  }

  test("OpensearchContainer.defaultTag is a valid semantic version") {
    OpensearchContainer.defaultTag should fullyMatch regex """^\d+\.\d+\.\d+$"""
  }
}
