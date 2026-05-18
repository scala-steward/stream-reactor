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

import io.lenses.sql.Field
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.ByteBuffer

/**
 * Exhaustive unit tests for [[Transform.apply]] covering the 8-branch schema-dispatch matrix
 * and pinning the 7 deliberate exception-message literals (including the BYTES vs STRING
 * `"Invalid json."` / `"Invalid json"` period asymmetry documented in the parity contract).
 */
class TransformTest extends AnyFunSuite with Matchers {

  private val allFields: Seq[Field] = Seq(Field("*", "*", null))

  // ---- Branch 0: null value → None regardless of schema --------------------------------

  test("null value with non-null schema returns None") {
    Transform(allFields, Schema.STRING_SCHEMA, null, withStructure = false) shouldBe None
  }

  test("null value with null schema returns None") {
    Transform(allFields, null, null, withStructure = false) shouldBe None
  }

  // ---- Branch 1: BYTES schema + Array[Byte] with valid JSON ----------------------------

  test("BYTES schema + Array[Byte] valid JSON returns Some(node)") {
    val json   = """{"a":1}""".getBytes("UTF-8")
    val result = Transform(allFields, Schema.BYTES_SCHEMA, json, withStructure = false)
    result.isDefined shouldBe true
    result.get.get("a").asInt() shouldBe 1
  }

  // ---- Branch 2: BYTES schema + ByteBuffer with valid JSON -----------------------------

  test("BYTES schema + ByteBuffer valid JSON returns Some(node)") {
    val json   = """{"b":2}""".getBytes("UTF-8")
    val result = Transform(allFields, Schema.BYTES_SCHEMA, ByteBuffer.wrap(json), withStructure = false)
    result.isDefined shouldBe true
    result.get.get("b").asInt() shouldBe 2
  }

  // ---- Branch 3: BYTES schema + invalid value type ------------------------------------

  test("""BYTES schema + invalid value type raises "Invalid payload:... for schema Schema.BYTES."""") {
    val ex = intercept[IllegalArgumentException] {
      Transform(allFields, Schema.BYTES_SCHEMA, 42, withStructure = false)
    }
    ex.getMessage should include("Invalid payload:42 for schema Schema.BYTES.")
  }

  // ---- Branch 4: BYTES schema + Array[Byte] with invalid JSON -------------------------
  // Note the period: "Invalid json." — distinct from the STRING path below.

  test("""BYTES schema + Array[Byte] invalid JSON raises "Invalid json." (with period)""") {
    val bad = "not-json".getBytes("UTF-8")
    val ex  = intercept[IllegalArgumentException](Transform(allFields, Schema.BYTES_SCHEMA, bad, withStructure = false))
    ex.getMessage shouldBe "Invalid json."
  }

  // ---- Branch 5: STRING schema + valid JSON -------------------------------------------

  test("STRING schema + valid JSON string returns Some(node)") {
    val result = Transform(allFields, Schema.STRING_SCHEMA, """{"c":3}""", withStructure = false)
    result.isDefined shouldBe true
    result.get.get("c").asInt() shouldBe 3
  }

  // ---- Branch 6: STRING schema + invalid JSON -----------------------------------------
  // Note NO period: "Invalid json" — deliberate parity asymmetry vs the BYTES path.

  test("""STRING schema + invalid JSON raises "Invalid json" (no period)""") {
    val ex = intercept[IllegalArgumentException](
      Transform(allFields, Schema.STRING_SCHEMA, "not-json", withStructure = false),
    )
    ex.getMessage shouldBe "Invalid json"
  }

  // ---- Branch 7: STRUCT schema --------------------------------------------------------

  test("STRUCT schema + valid Struct value returns Some(node)") {
    val schema = SchemaBuilder.struct().field("d", Schema.INT32_SCHEMA).build()
    val struct = new Struct(schema).put("d", 99)
    val result = Transform(allFields, schema, struct, withStructure = false)
    result.isDefined shouldBe true
    result.get.get("d").asInt() shouldBe 99
  }

  // ---- Branch 8: unsupported schema type ----------------------------------------------

  test("""Unsupported schema type raises "Can't transform Schema type:..." (with period)""") {
    val ex = intercept[IllegalArgumentException](
      Transform(allFields, Schema.INT32_SCHEMA, 5, withStructure = false),
    )
    ex.getMessage should startWith("Can't transform Schema type:INT32.")
  }

  // ---- Schemaless dispatch (schema == null) -------------------------------------------

  test("Schemaless Map[String,Any] returns Some(node)") {
    val map: java.util.Map[String, Any] = new java.util.HashMap()
    map.put("e", 5.asInstanceOf[Any])
    val result = Transform(allFields, null, map, withStructure = false)
    result.isDefined shouldBe true
    result.get.get("e").asInt() shouldBe 5
  }

  test("Schemaless valid JSON String returns Some(node)") {
    val result = Transform(allFields, null, """{"f":6}""", withStructure = false)
    result.isDefined shouldBe true
    result.get.get("f").asInt() shouldBe 6
  }

  test("""Schemaless invalid JSON String raises "Invalid json." (with period)""") {
    val ex = intercept[IllegalArgumentException](Transform(allFields, null, "bad", withStructure = false))
    ex.getMessage shouldBe "Invalid json."
  }

  test("Schemaless Array[Byte] valid JSON returns Some(node)") {
    val bytes  = """{"g":7}""".getBytes("UTF-8")
    val result = Transform(allFields, null, bytes, withStructure = false)
    result.isDefined shouldBe true
    result.get.get("g").asInt() shouldBe 7
  }

  test("""Schemaless Array[Byte] invalid JSON raises "Invalid json." (with period)""") {
    val bad = "bad".getBytes("UTF-8")
    val ex  = intercept[IllegalArgumentException](Transform(allFields, null, bad, withStructure = false))
    ex.getMessage shouldBe "Invalid json."
  }

  test("""Schemaless unsupported type raises "Value:... is not handled!" """) {
    val ex = intercept[IllegalArgumentException](Transform(allFields, null, List(1, 2, 3), withStructure = false))
    ex.getMessage should include("is not handled!")
  }
}
