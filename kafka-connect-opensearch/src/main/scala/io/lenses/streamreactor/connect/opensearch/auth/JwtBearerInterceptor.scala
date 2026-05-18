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

import org.apache.hc.core5.http.EntityDetails
import org.apache.hc.core5.http.HttpRequest
import org.apache.hc.core5.http.HttpRequestInterceptor
import org.apache.hc.core5.http.protocol.HttpContext

/**
 * Apache HTTP client 5 interceptor that injects a JWT Bearer token into the Authorization header.
 *
 * Uses `setHeader` (not `addHeader`) so that any pre-existing Authorization header is replaced,
 * not augmented.
 */
class JwtBearerInterceptor(tokenSource: JwtTokenSource) extends HttpRequestInterceptor {

  override def process(request: HttpRequest, entityDetails: EntityDetails, context: HttpContext): Unit =
    request.setHeader("Authorization", s"Bearer ${tokenSource.getToken}")
}
