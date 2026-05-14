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
package io.lenses.streamreactor.connect.elastic.common

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class NullValueBehaviorTest extends AnyFunSuite with Matchers {

  test("fromString returns FAIL for exact uppercase input") {
    NullValueBehavior.fromString("FAIL") shouldBe NullValueBehavior.FAIL
  }

  test("fromString returns DELETE for exact uppercase input") {
    NullValueBehavior.fromString("DELETE") shouldBe NullValueBehavior.DELETE
  }

  test("fromString returns IGNORE for exact uppercase input") {
    NullValueBehavior.fromString("IGNORE") shouldBe NullValueBehavior.IGNORE
  }

  test("fromString is case-insensitive: lowercase 'delete' resolves to DELETE") {
    NullValueBehavior.fromString("delete") shouldBe NullValueBehavior.DELETE
  }

  test("fromString is case-insensitive: lowercase 'fail' resolves to FAIL") {
    NullValueBehavior.fromString("fail") shouldBe NullValueBehavior.FAIL
  }

  test("fromString is case-insensitive: mixed-case 'Delete' resolves to DELETE") {
    NullValueBehavior.fromString("Delete") shouldBe NullValueBehavior.DELETE
  }

  test("fromString returns IGNORE for an unknown value") {
    NullValueBehavior.fromString("NOOP") shouldBe NullValueBehavior.IGNORE
  }

  test("fromString returns IGNORE for null without throwing NPE") {
    NullValueBehavior.fromString(null) shouldBe NullValueBehavior.IGNORE
  }

  test("fromString returns IGNORE for empty string") {
    NullValueBehavior.fromString("") shouldBe NullValueBehavior.IGNORE
  }
}
