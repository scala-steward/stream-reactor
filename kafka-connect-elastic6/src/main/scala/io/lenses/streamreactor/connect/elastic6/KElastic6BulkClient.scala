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
import io.lenses.streamreactor.connect.elastic.common.bulk.BulkItemError
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
 * @param writeTimeoutMs timeout in milliseconds, matching the config key `connect.elastic.write.timeout`
 *                       (documented in millis, default 300000 = 5 minutes).
 */
class KElastic6BulkClient(client: KElasticClient, writeTimeoutMs: Int) extends KBulkClient with StrictLogging {

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

    val response = Await.result(client.execute(ElasticDsl.bulk(elasticRequests)), writeTimeoutMs.milliseconds)

    if (response.isError) {
      throw new RuntimeException(s"Elastic bulk transport error: ${response.error.reason}")
    }

    val result     = response.result
    val tookMillis = result.took

    val itemErrors: Seq[BulkItemError] = result.items
      .filter(_.error.isDefined)
      .map(item => BulkItemError(item.index, item.id, item.error.map(_.reason).getOrElse("")))

    if (itemErrors.nonEmpty) {
      logger.warn(
        s"Bulk write completed with ${itemErrors.size} item-level errors: " +
          itemErrors.map(e => s"[${e.index}/${e.id}] ${e.reason}").mkString(", "),
      )
    }

    logger.info(s"Bulk write completed: took=${tookMillis}ms, items=${result.items.size}")
    BulkResult(took = tookMillis, errors = itemErrors.nonEmpty, itemErrors = itemErrors)
  }

  override def createIndex(name: String): Try[Unit] = Try {
    client.createIndex(name)
  }

  override def close(): Unit = client.close()
}
