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

import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchSettings
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider

/**
 * Factory for AWS credentials providers based on [[OpenSearchSettings]].
 *
 * Called exactly once per `OpenSearchSinkTask.start()`, NOT per bulk.
 */
object AwsCredentialsProviderFactory extends StrictLogging {

  def create(settings: OpenSearchSettings): AwsCredentialsProvider =
    settings.awsCredentialsProvider match {
      case "STATIC" =>
        val credentials =
          if (settings.awsSessionToken.nonEmpty)
            AwsSessionCredentials.create(settings.awsAccessKeyId, settings.awsSecretAccessKey, settings.awsSessionToken)
          else
            AwsBasicCredentials.create(settings.awsAccessKeyId, settings.awsSecretAccessKey)
        StaticCredentialsProvider.create(credentials)

      case _ =>
        DefaultCredentialsProvider.create()
    }
}
