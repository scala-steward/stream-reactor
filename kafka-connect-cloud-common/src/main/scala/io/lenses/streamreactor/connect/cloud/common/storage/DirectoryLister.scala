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
package io.lenses.streamreactor.connect.cloud.common.storage

import cats.effect.IO
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation

trait DirectoryLister {

  /**
   * Finds the partition directories beneath the given location.
   *
   * `partitionDepth` is the number of directory levels below the configured prefix at which partition directories
   * live, and it means the same thing on every cloud:
   *
   *   - 0 - the prefix itself is the single partition directory.
   *   - 1 - the immediate children of the prefix.
   *   - n - n levels below the prefix.
   *
   * `exclude` and `wildcardExcludes` are applied at every level.  Task ownership is applied only at the leaf level,
   * so a directory is not dropped because an ancestor of it hashes to a different task.
   *
   * @param wildcardExcludes    allows ignoring paths containing certain strings.  Mainly it is used to prevent us from reading anything inside the .indexes key prefix, as these should be ignored by the source.
   * @param prefixAsConfigured  searches from the prefix exactly as configured rather than from the directory it names, which is how the deprecated `recurse.levels` behaved on S3.  A prefix without a trailing slash then spends its first level reaching the directory it names, and matches sibling directories sharing its name, because the cloud treats it as a key prefix rather than a directory.  See LC-316.
   */
  def findDirectories(
    bucketAndPrefix:    CloudLocation,
    filesLimit:         Int,
    partitionDepth:     Int,
    exclude:            Set[String],
    wildcardExcludes:   Set[String],
    prefixAsConfigured: Boolean = false,
  ): IO[Set[String]]
}
