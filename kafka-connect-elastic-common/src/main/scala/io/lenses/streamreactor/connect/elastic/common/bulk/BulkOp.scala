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

import com.fasterxml.jackson.databind.JsonNode

sealed trait BulkOp {
  def index:        String
  def id:           String
  def documentType: Option[String]
}

final case class InsertOp(
  index:        String,
  id:           String,
  json:         JsonNode,
  pipeline:     Option[String],
  documentType: Option[String] = None,
) extends BulkOp

final case class UpsertOp(
  index:        String,
  id:           String,
  json:         JsonNode,
  documentType: Option[String] = None,
) extends BulkOp

final case class DeleteOp(
  index:        String,
  id:           String,
  documentType: Option[String] = None,
) extends BulkOp
