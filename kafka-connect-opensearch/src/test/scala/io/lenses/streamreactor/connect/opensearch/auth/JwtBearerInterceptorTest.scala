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
package io.lenses.streamreactor.connect.opensearch.auth

import org.apache.hc.core5.http.message.BasicHttpRequest
import org.apache.hc.core5.http.Method
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class JwtBearerInterceptorTest extends AnyFunSuite with Matchers {

  private def newRequest: BasicHttpRequest = new BasicHttpRequest(Method.POST, "/_bulk")

  test("injects Authorization: Bearer <token> header") {
    val source      = new StaticJwtTokenSource("test-token-123")
    val interceptor = new JwtBearerInterceptor(source)
    val request     = newRequest
    interceptor.process(request, null, null)
    request.getFirstHeader("Authorization").getValue shouldBe "Bearer test-token-123"
  }

  test("replaces a pre-existing Authorization header (setHeader, not addHeader)") {
    val source      = new StaticJwtTokenSource("new-token")
    val interceptor = new JwtBearerInterceptor(source)
    val request     = newRequest
    request.setHeader("Authorization", "Bearer old-token")
    interceptor.process(request, null, null)
    // exactly one Authorization header after processing
    request.getHeaders("Authorization") should have length 1
    request.getFirstHeader("Authorization").getValue shouldBe "Bearer new-token"
  }

  test("token value is not visible in request toString") {
    val secretToken = "super-secret-jwt-value"
    val source      = new StaticJwtTokenSource(secretToken)
    val interceptor = new JwtBearerInterceptor(source)
    val request     = newRequest
    interceptor.process(request, null, null)
    // BasicHttpRequest.toString() only shows the request line (method + URI), not headers.
    // The token must NOT appear in the request-line string.
    request.toString should not include secretToken
  }
}
