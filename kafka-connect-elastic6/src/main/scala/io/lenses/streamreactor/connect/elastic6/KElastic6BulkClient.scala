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
package io.lenses.streamreactor.connect.elastic6

import com.sksamuel.elastic4s.Index
import com.sksamuel.elastic4s.http.ElasticDsl
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.connect.elastic.common.bulk.BulkOp
import io.lenses.streamreactor.connect.elastic.common.bulk.BulkResult
import io.lenses.streamreactor.connect.elastic.common.bulk.DeleteOp
import io.lenses.streamreactor.connect.elastic.common.bulk.InsertOp
import io.lenses.streamreactor.connect.elastic.common.bulk.KBulkClient
import io.lenses.streamreactor.connect.elastic.common.bulk.UpsertOp

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

/**
 * Adapts the elastic4s-6-based [[KElasticClient]] to the [[KBulkClient]] trait.
 *
 * Elastic 6 requires a document type in index/update/delete operations.
 * The fallback (per original [[ElasticJsonWriter]] behaviour) is to use the index name
 * as the document type when the KCQL `WITHDOCTYPE` clause is absent.
 *
 * Error handling preserves ES6 parity with the pre-refactor [[ElasticJsonWriter]]:
 * HTTP-transport errors are surfaced via the returned `Try`; per-item bulk errors
 * (e.g. mapping conflicts, version conflicts) are logged at WARN but treated as
 * non-fatal, matching ES7 behaviour.
 *
 * @param writeTimeoutSeconds timeout in **seconds**, preserving the pre-refactor ES6 interpretation of
 *                            `connect.elastic.write.timeout` (default 300000 ≈ 83 hours, effectively
 *                            unbounded). Despite the config doc historically claiming "millis", the old
 *                            `ElasticJsonWriter` always passed this value to `Await.result` as `.seconds`.
 *                            We preserve that here to avoid breaking existing deployments. OpenSearch uses
 *                            milliseconds instead (see OpenSearchTransportFactory).
 */
class KElastic6BulkClient(client: KElasticClient, writeTimeoutSeconds: Int) extends KBulkClient with StrictLogging {

  override def supportsDocumentType: Boolean = true

  override def bulk(ops: Seq[BulkOp]): Try[BulkResult] = Try {
    import ElasticDsl._

    val elasticRequests = ops.map {
      case InsertOp(index, id, json, pipeline, documentType) =>
        val docType = documentType.getOrElse(index)
        indexInto(index / docType)
          .id(id)
          .pipeline(pipeline.orNull)
          .source(json.toString)

      case UpsertOp(index, id, json, documentType) =>
        val docType = documentType.getOrElse(index)
        update(id)
          .in(index / docType)
          .docAsUpsert(json.toString)

      case DeleteOp(index, id, documentType) =>
        val docType = documentType.getOrElse(index)
        deleteById(new Index(index), docType, id)
    }

    val response = Await.result(client.execute(ElasticDsl.bulk(elasticRequests)), writeTimeoutSeconds.seconds)

    if (response.isError) {
      throw new RuntimeException(s"Elastic bulk transport error: ${response.error.reason}")
    }

    val result     = response.result
    val tookMillis = result.took

    // ES6 parity: item-level errors are logged at WARN but treated as non-fatal,
    // matching the pre-refactor ElasticJsonWriter behaviour and the ES7 client.
    val itemErrorMessages = result.items
      .filter(_.error.isDefined)
      .map(item => s"[${item.index}/${item.id}] ${item.error.map(_.reason).getOrElse("")}")

    if (itemErrorMessages.nonEmpty) {
      logger.warn(
        s"Bulk write completed with ${itemErrorMessages.size} item-level errors (ES6 tolerant mode): $itemErrorMessages",
      )
    }

    logger.info(s"Bulk write completed: took=${tookMillis}ms, items=${result.items.size}")
    BulkResult(took = tookMillis, errors = false, itemErrors = Seq.empty)
  }

  override def createIndex(name: String): Try[Unit] = Try {
    client.createIndex(name)
  }

  override def close(): Unit = client.close()
}
