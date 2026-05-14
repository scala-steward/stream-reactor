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
package io.lenses.streamreactor.connect.elastic7

import com.sksamuel.elastic4s.ElasticDsl
import com.sksamuel.elastic4s.Index
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
 * Adapts the elastic4s-based [[KElasticClient]] to the [[KBulkClient]] trait used by the shared [[io.lenses.streamreactor.connect.elastic.common.writer.JsonBulkWriter]].
 *
 * Error handling preserves ES7 parity: HTTP-transport errors are surfaced via the returned `Try`; per-item
 * bulk errors (e.g. mapping conflicts) are silently dropped (logged at WARN) to match the original
 * [[ElasticJsonWriter]] behaviour.
 *
 * @param writeTimeoutMs timeout in milliseconds, matching the config key `connect.elastic.write.timeout`
 *                       (documented in millis, default 300000 = 5 minutes).
 */
class KElasticBulkClient(client: KElasticClient, writeTimeoutMs: Int) extends KBulkClient with StrictLogging {

  override def bulk(ops: Seq[BulkOp]): Try[BulkResult] = Try {
    import ElasticDsl._
    val elasticRequests = ops.map {
      case InsertOp(index, id, json, pipeline, _) =>
        indexInto(new Index(index))
          .id(id)
          .pipeline(pipeline.orNull)
          .source(json.toString)

      case UpsertOp(index, id, json, _) =>
        updateById(new Index(index), id)
          .docAsUpsert(json.toString)

      case DeleteOp(index, id, _) =>
        deleteById(new Index(index), id)
    }

    val response = Await.result(client.execute(ElasticDsl.bulk(elasticRequests)), writeTimeoutMs.milliseconds)

    if (response.isError) {
      throw new RuntimeException(s"Elastic bulk transport error: ${response.error.reason}")
    }

    val result     = response.result
    val tookMillis = result.took

    // ES7 parity: item-level errors are logged at WARN but treated as non-fatal (tolerant mode).
    val itemErrorMessages = result.items
      .filter(_.error.isDefined)
      .map(item => s"[${item.index}/${item.id}] ${item.error.map(_.reason).getOrElse("")}")

    if (itemErrorMessages.nonEmpty) {
      logger.warn(
        s"Bulk write completed with ${itemErrorMessages.size} item-level errors (ES7 tolerant mode): $itemErrorMessages",
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
