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
package io.lenses.streamreactor.connect.aws.s3.storage

import cats.effect.IO
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.storage.DepthDirectoryLister
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model._

import scala.jdk.CollectionConverters.IteratorHasAsScala

class AwsS3DirectoryLister(connectorTaskId: ConnectorTaskId, s3Client: S3Client)
    extends DepthDirectoryLister(connectorTaskId) {

  override protected def listChildDirectories(bucket: String, prefix: String, filesLimit: Int): IO[Set[String]] =
    IO {
      val builder = ListObjectsV2Request
        .builder()
        .maxKeys(filesLimit)
        .bucket(bucket)
        .delimiter("/")
      if (prefix.nonEmpty) builder.prefix(prefix)

      s3Client
        .listObjectsV2Paginator(builder.build())
        .iterator()
        .asScala
        .foldLeft(Set.empty[String]) {
          case (acc, listResp) =>
            // With a delimiter set, S3 collapses everything below the next `/` into a common prefix, so the common
            // prefixes are the child directories and `contents` holds only the objects directly in this one.  Each
            // prefix is the full key up to and including that `/`, which is the directory path we want.  Listing
            // `topic-1/` over keys `topic-1/0/1.avro`, `topic-1/0/2.avro` and `topic-1/1/3.avro` yields the common
            // prefixes `topic-1/0/` and `topic-1/1/`.
            acc ++ Option(listResp.commonPrefixes())
              .map(_.iterator().asScala.map(_.prefix()).toSet)
              .getOrElse(Set.empty)
        }
    }
}
