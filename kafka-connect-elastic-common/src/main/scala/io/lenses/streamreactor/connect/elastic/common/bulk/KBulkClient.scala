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
package io.lenses.streamreactor.connect.elastic.common.bulk

import scala.util.Success
import scala.util.Try

trait KBulkClient extends AutoCloseable {

  /**
   * Lifecycle hook called once at writer construction.
   * Implementations may probe the remote service (e.g. cluster-compatibility check)
   * and return Failure to surface misconfiguration before the first record is processed.
   * The default is a no-op Success.
   */
  def start(): Try[Unit] = Success(())

  /**
   * Whether this backend honours the `WITHDOCTYPE` KCQL clause.
   *
   * Elasticsearch 6 requires a mapping type in every index/update/delete request and therefore
   * uses the doc type extracted from the KCQL clause (falling back to the index name when absent).
   * Elasticsearch 7 and OpenSearch dropped mapping types; those clients always ignore the clause.
   * The shared [[io.lenses.streamreactor.connect.elastic.common.writer.JsonBulkWriter]] uses this
   * flag to decide whether to emit a warning when WITHDOCTYPE is present.
   *
   * Default: `false` (ignored).
   */
  def supportsDocumentType: Boolean = false

  def bulk(ops:         Seq[BulkOp]): Try[BulkResult]
  def createIndex(name: String):      Try[Unit]
}
