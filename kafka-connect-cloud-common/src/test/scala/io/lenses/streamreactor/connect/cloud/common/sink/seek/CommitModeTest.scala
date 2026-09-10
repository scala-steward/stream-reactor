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
package io.lenses.streamreactor.connect.cloud.common.sink.seek

import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CommitModeTest extends AnyFunSuite with Matchers with EitherValues {

  test("[B] T1.1 CommitMode.fromString accepts granular and batch case-insensitively") {
    Seq("granular", "GRANULAR", "Granular", " granular ").foreach { s =>
      CommitMode.fromString(s).value shouldBe CommitMode.Granular
    }
    Seq("batch", "BATCH", "Batch", " Batch ").foreach { s =>
      CommitMode.fromString(s).value shouldBe CommitMode.Batch
    }
  }

  test("[B] T1.1 CommitMode.fromString rejects any other value with a message naming the valid values") {
    val err = CommitMode.fromString("bogus").left.value
    err should include("bogus")
    err should include("granular")
    err should include("batch")
  }

  test("[B] T1.1 the default commit mode is granular") {
    CommitMode.Default shouldBe CommitMode.Granular
  }
}
