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
package io.lenses.streamreactor.connect.gcp.storage.storage

import cats.effect.IO
import com.google.cloud.storage.Storage
import com.google.cloud.storage.Storage.BlobListOption
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.storage.DepthDirectoryLister

import scala.jdk.CollectionConverters.IterableHasAsScala

class GCPStorageDirectoryLister(connectorTaskId: ConnectorTaskId, storage: Storage)
    extends DepthDirectoryLister(connectorTaskId) {

  override protected def listChildDirectories(bucket: String, prefix: String, filesLimit: Int): IO[Set[String]] =
    IO {
      val blobListOptions = BlobListOption.dedupe(
        BlobListOption.delimiter("/"),
        BlobListOption.pageSize(filesLimit.toLong),
        BlobListOption.prefix(prefix),
        BlobListOption.currentDirectory(),
      )

      storage
        .get(bucket)
        .list(blobListOptions: _*)
        .iterateAll()
        .asScala
        .filter(_.isDirectory)
        .map(_.getName)
        .toSet
    }
}
