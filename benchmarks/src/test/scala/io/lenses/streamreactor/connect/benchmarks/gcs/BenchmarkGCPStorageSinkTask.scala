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
package io.lenses.streamreactor.connect.benchmarks.gcs

import com.google.cloud.storage.Storage
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import io.lenses.streamreactor.connect.gcp.storage.sink.GCPStorageSinkTask
import io.lenses.streamreactor.connect.gcp.storage.sink.config.GCPStorageSinkConfig
import io.lenses.streamreactor.connect.gcp.storage.storage.GCPStorageFileMetadata

/**
 * The real `GCPStorageSinkTask` with only the storage seam swapped out. `createClient` is left
 * as production code: with `connect.gcpstorage.gcp.auth.mode=none` it builds a real (but never
 * called) `com.google.cloud.storage.Storage` client without touching the network or requiring
 * credentials, exactly as `GCPProxyContainerTest`'s "none" auth mode does. Every other code path
 * -- config parsing, `WriterManager`, `IndexManagerV2`, `JsonFormatWriter`, commit policy
 * evaluation -- is unmodified production code.
 */
class BenchmarkGCPStorageSinkTask(storage: NoOpGCPStorage) extends GCPStorageSinkTask {

  override def createStorageInterface(
    connectorTaskId: ConnectorTaskId,
    config:          GCPStorageSinkConfig,
    cloudClient:     Storage,
  ): StorageInterface[GCPStorageFileMetadata] = storage

}
