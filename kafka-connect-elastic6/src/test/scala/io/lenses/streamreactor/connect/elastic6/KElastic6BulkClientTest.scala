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

import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.sksamuel.elastic4s.bulk.BulkRequest
import com.sksamuel.elastic4s.http.Response
import com.sksamuel.elastic4s.http.bulk.BulkError
import com.sksamuel.elastic4s.http.bulk.BulkResponse
import com.sksamuel.elastic4s.http.bulk.BulkResponseItem
import com.sksamuel.elastic4s.http.bulk.BulkResponseItems
import io.lenses.streamreactor.connect.elastic.common.bulk.InsertOp
import org.mockito.ArgumentMatchersSugar
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Future

class KElastic6BulkClientTest extends AnyWordSpec with Matchers with MockitoSugar with ArgumentMatchersSugar {

  private val sampleOps = Seq(
    InsertOp(
      index        = "myindex",
      id           = "doc1",
      json         = JsonNodeFactory.instance.objectNode().put("field", "value"),
      pipeline     = None,
      documentType = None,
    ),
  )

  "KElastic6BulkClient.bulk" should {

    "return errors=false and empty itemErrors when all items succeed" in {
      val (client, elasticClient) = setup()

      val successItem = bulkResponseItem(id = "doc1", index = "myindex", error = None)
      val bulkResp =
        BulkResponse(took = 5L, errors = false, _items = Seq(BulkResponseItems(Some(successItem), None, None, None)))
      when(elasticClient.result).thenReturn(bulkResp)

      val result = client.bulk(sampleOps)
      result.isSuccess shouldBe true
      result.get.errors shouldBe false
      result.get.itemErrors shouldBe empty
    }

    "log item-level errors but return errors=false (ES6 tolerant mode / ES7 parity)" in {
      val (client, elasticClient) = setup()

      val err = BulkError(`type` = "mapper_parsing_exception",
                          reason     = "failed to parse field [foo]",
                          index_uuid = "abc",
                          shard      = 0,
                          index      = "myindex",
      )
      val errItem = bulkResponseItem(id = "doc1", index = "myindex", error = Some(err))
      val bulkResp =
        BulkResponse(took = 3L, errors = true, _items = Seq(BulkResponseItems(Some(errItem), None, None, None)))
      when(elasticClient.result).thenReturn(bulkResp)

      val result = client.bulk(sampleOps)
      result.isSuccess shouldBe true
      val br = result.get
      br.errors shouldBe false
      br.itemErrors shouldBe empty
    }

    "log multiple item-level errors but return errors=false regardless of transport errors flag" in {
      val (client, elasticClient) = setup()

      val err1 =
        BulkError(`type` = "version_conflict", reason = "version conflict", index_uuid = "x", shard = 0, index = "idx")
      val err2 = BulkError(`type` = "version_conflict",
                           reason     = "version conflict 2",
                           index_uuid = "x",
                           shard      = 0,
                           index      = "idx",
      )
      val errItem1 = bulkResponseItem(id = "doc1", index = "idx", error = Some(err1))
      val errItem2 = bulkResponseItem(id = "doc2", index = "idx", error = Some(err2))
      val bulkResp = BulkResponse(
        took   = 2L,
        errors = false,
        _items =
          Seq(BulkResponseItems(Some(errItem1), None, None, None), BulkResponseItems(Some(errItem2), None, None, None)),
      )
      when(elasticClient.result).thenReturn(bulkResp)

      val result = client.bulk(sampleOps)
      result.isSuccess shouldBe true
      val br = result.get
      br.errors shouldBe false
      br.itemErrors shouldBe empty
    }

    "surface a transport-level error as a Failure" in {
      val elasticClient = mock[KElasticClient]
      val mockResp      = mock[Response[BulkResponse]]
      when(elasticClient.execute(any[BulkRequest])).thenReturn(Future.successful(mockResp))
      when(mockResp.isError).thenReturn(true)
      when(mockResp.error).thenReturn(
        com.sksamuel.elastic4s.http.ElasticError("transport_error",
                                                 "could not connect",
                                                 None,
                                                 None,
                                                 None,
                                                 Seq.empty,
                                                 None,
        ),
      )

      val client = new KElastic6BulkClient(elasticClient, writeTimeoutMs = 5000)
      val result = client.bulk(sampleOps)
      result.isFailure shouldBe true
      result.failed.get.getMessage should include("transport error")
    }
  }

  private def setup(): (KElastic6BulkClient, Response[BulkResponse]) = {
    val elasticClient = mock[KElasticClient]
    val mockResp      = mock[Response[BulkResponse]]
    when(elasticClient.execute(any[BulkRequest])).thenReturn(Future.successful(mockResp))
    when(mockResp.isError).thenReturn(false)
    val client = new KElastic6BulkClient(elasticClient, writeTimeoutMs = 5000)
    (client, mockResp)
  }

  private def bulkResponseItem(id: String, index: String, error: Option[BulkError]): BulkResponseItem =
    BulkResponseItem(
      itemId        = 0,
      id            = id,
      index         = index,
      `type`        = "_doc",
      version       = 1L,
      seqNo         = 0L,
      primaryTerm   = 1L,
      forcedRefresh = false,
      found         = false,
      created       = error.isEmpty,
      result        = if (error.isDefined) "error" else "created",
      status        = if (error.isDefined) 400 else 201,
      error         = error,
      shards        = None,
    )
}
