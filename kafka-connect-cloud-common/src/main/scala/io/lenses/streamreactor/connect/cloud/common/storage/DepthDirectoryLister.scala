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
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation

/**
 * Walks the directory tree beneath a prefix down to `partitionDepth`, so that the same depth means the same
 * thing on every cloud.  Subclasses supply only the one-level listing primitive for their storage client.
 */
abstract class DepthDirectoryLister(connectorTaskId: ConnectorTaskId) extends DirectoryLister with LazyLogging {

  /**
   * Lists the directories directly beneath `prefix`, one level down only.  Returned paths are full keys relative to
   * the bucket root, each with a trailing slash.
   */
  protected def listChildDirectories(bucket: String, prefix: String, filesLimit: Int): IO[Set[String]]

  override def findDirectories(
    bucketAndPrefix:    CloudLocation,
    filesLimit:         Int,
    partitionDepth:     Int,
    exclude:            Set[String],
    wildcardExcludes:   Set[String],
    prefixAsConfigured: Boolean = false,
  ): IO[Set[String]] = {

    // Exclusions apply at every level so an excluded directory is never descended into, but ownership applies only at
    // the leaf, otherwise a directory would be dropped because an ancestor of it hashed to a different task.
    def keep(dir: String, isLeaf: Boolean): Boolean =
      !exclude.contains(dir) && !wildcardExcludes.exists(dir.contains) &&
        (!isLeaf || connectorTaskId.ownsDir(dir))

    // Depth 0 makes the starting prefix itself the leaf, so it is filtered like any other discovered directory.
    def walk(prefix: String, levelsRemaining: Int): IO[Set[String]] =
      if (levelsRemaining <= 0) IO.pure(Set(prefix).filter(keep(_, isLeaf = true)))
      else
        listChildDirectories(bucketAndPrefix.bucket, prefix, filesLimit)
          .map(_.filter(keep(_, isLeaf = levelsRemaining == 1)))
          .flatTap { found =>
            IO.delay(
              logger.trace(
                s"[$connectorTaskId] Searching directory $prefix at $levelsRemaining level(-s) from the leaf, found ${found.size}",
              ),
            )
          }
          .flatMap(_.toList.traverse(walk(_, levelsRemaining - 1)).map(_.toSet.flatten))

    val configuredPrefix = bucketAndPrefix.prefixOrDefault()
    val startingPrefix =
      if (prefixAsConfigured) configuredPrefix else DepthDirectoryLister.ensureTrailingSlash(configuredPrefix)

    walk(startingPrefix, partitionDepth)
  }
}

object DepthDirectoryLister {

  /**
   * An empty string is a valid path at the root of the bucket, so it is left untouched rather than turned into a
   * lone slash.
   */
  def ensureTrailingSlash(prefix: String): String =
    if (prefix.isEmpty || prefix.endsWith("/")) prefix else s"$prefix/"
}
