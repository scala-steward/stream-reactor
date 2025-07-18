/*
 * Copyright 2017-2025 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.azure.cosmosdb

import com.fasterxml.jackson.databind.ObjectMapper
import org.json4s._
import org.json4s.native.JsonMethods._
import com.fasterxml.jackson.module.scala.DefaultScalaModule
object Json {
  implicit val formats: Formats = DefaultFormats

  private val mapper = new ObjectMapper
  mapper.registerModule(DefaultScalaModule)

  def fromJson[T <: Product: Manifest](json: String): T =
    parse(json).extract[T]

  def toJson[T](t: T): String =
    mapper.writeValueAsString(t)
}
