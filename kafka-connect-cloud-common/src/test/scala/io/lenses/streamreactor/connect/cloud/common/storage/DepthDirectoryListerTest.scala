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
import cats.effect.unsafe.implicits.global
import cats.implicits.catsSyntaxOptionId
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.utils.SampleData
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DepthDirectoryListerTest extends AnyFlatSpec with Matchers {

  private implicit val cloudLocationValidator: CloudLocationValidator = SampleData.cloudLocationValidator

  private val bucket     = "bucket"
  private val filesLimit = 1000

  /**
   * Keys are held as a flat set, exactly as an object store holds them, so that the one-level listing primitive has to
   * derive the child directories the same way the cloud clients do.
   */
  private class InMemoryLister(keys: Set[String], connectorTaskId: ConnectorTaskId)
      extends DepthDirectoryLister(connectorTaskId) {

    var listedPrefixes: List[String] = List.empty

    override protected def listChildDirectories(
      bucket:     String,
      prefix:     String,
      filesLimit: Int,
    ): IO[Set[String]] =
      IO {
        listedPrefixes = listedPrefixes :+ prefix
        keys
          .filter(_.startsWith(prefix))
          .flatMap { key =>
            val remainder = key.drop(prefix.length)
            remainder.indexOf('/') match {
              case -1  => Option.empty[String]
              case idx => Some(prefix + remainder.take(idx + 1))
            }
          }
      }
  }

  private def find(
    keys:               Set[String],
    prefix:             Option[String],
    partitionDepth:     Int,
    exclude:            Set[String]     = Set.empty,
    wildcardExcludes:   Set[String]     = Set.empty,
    connectorTaskId:    ConnectorTaskId = ConnectorTaskId("sourceName", 1, 0),
    prefixAsConfigured: Boolean         = false,
  ): Set[String] =
    new InMemoryLister(keys, connectorTaskId)
      .findDirectories(
        CloudLocation(bucket, prefix),
        filesLimit,
        partitionDepth,
        exclude,
        wildcardExcludes,
        prefixAsConfigured,
      )
      .unsafeRunSync()

  private val topicPartitionKeys: Set[String] = Set(
    "prefix/topicA/0000001/file.avro",
    "prefix/topicA/0000002/file.avro",
    "prefix/topicB/0000001/file.avro",
  )

  "findDirectories" should "return the prefix itself at depth 0" in {
    find(topicPartitionKeys, "prefix/".some, 0) should be(Set("prefix/"))
  }

  "findDirectories" should "return the immediate children at depth 1" in {
    find(topicPartitionKeys, "prefix/".some, 1) should be(Set("prefix/topicA/", "prefix/topicB/"))
  }

  "findDirectories" should "return the partition directories at depth 2" in {
    find(topicPartitionKeys, "prefix/".some, 2) should be(
      Set("prefix/topicA/0000001/", "prefix/topicA/0000002/", "prefix/topicB/0000001/"),
    )
  }

  "findDirectories" should "treat an absent prefix as the bucket root" in {
    find(topicPartitionKeys, None, 1) should be(Set("prefix/"))
  }

  "findDirectories" should "append a trailing slash to a prefix that lacks one" in {
    find(topicPartitionKeys, "prefix".some, 1) should be(Set("prefix/topicA/", "prefix/topicB/"))
  }

  "findDirectories" should "apply wildcard excludes at every level, not just the leaf" in {
    val keys = topicPartitionKeys ++ Set(".indexes/sourceName/topicA/0000001/0000000000000000050")

    val lister = new InMemoryLister(keys, ConnectorTaskId("sourceName", 1, 0))
    val found = lister
      .findDirectories(CloudLocation(bucket, None), filesLimit, 3, Set.empty, Set(".indexes"))
      .unsafeRunSync()

    found should be(Set("prefix/topicA/0000001/", "prefix/topicA/0000002/", "prefix/topicB/0000001/"))
    lister.listedPrefixes should not contain ".indexes/"
  }

  "findDirectories" should "exclude directories named in the exclude set" in {
    find(topicPartitionKeys, "prefix/".some, 2, exclude = Set("prefix/topicA/0000001/")) should be(
      Set("prefix/topicA/0000002/", "prefix/topicB/0000001/"),
    )
  }

  "findDirectories" should "exclude the prefix itself at depth 0 when it is already known" in {
    find(topicPartitionKeys, "prefix/".some, 0, exclude = Set("prefix/")) should be(Set.empty)
  }

  // The shape of the S3 source envelope integration tests: a single object sitting directly in the configured prefix.
  // Searching from the prefix as configured spends the first level arriving at the directory, so it lands one level
  // shallower than searching from the directory itself, and a prefix that already ends in a slash is unaffected.
  // See LC-316.
  "findDirectories" should "land one level shallower when searching from a prefix without a trailing slash" in {
    val keys = Set("backups/avro/0")

    find(keys, "backups/avro".some, 1) should be(Set.empty)
    find(keys, "backups/avro".some, 1, prefixAsConfigured = true) should be(Set("backups/avro/"))

    find(topicPartitionKeys, "prefix/".some, 2, prefixAsConfigured = true) should be(
      Set("prefix/topicA/0000001/", "prefix/topicA/0000002/", "prefix/topicB/0000001/"),
    )
  }

  // The old S3 lister passed the prefix to the cloud as a key prefix rather than a directory, so a prefix of `bytes`
  // also matched a sibling directory named `bytesval`.  Configurations using the deprecated key still rely on it.
  // InMemoryLister models that with startsWith; the real S3 semantics are covered by S3SourceTaskTest.
  "findDirectories" should "match sibling directories sharing the prefix only when searching from the prefix as configured" in {
    val keys = Set("backups/bytesval/myTopic/0/199.bytes")

    find(keys, "backups/bytes".some, 1, prefixAsConfigured = true) should be(Set("backups/bytesval/"))
    find(keys, "backups/bytes".some, 1) should be(Set.empty)
  }

  "findDirectories" should "omit a branch that holds files but no subdirectory" in {
    val keys = Set(
      "prefix/topicA/0000001/file.avro",
      "prefix/topicB/file.avro",
    )

    find(keys, "prefix/".some, 2) should be(Set("prefix/topicA/0000001/"))
  }

  "findDirectories" should "apply ownership only at the leaf level so every directory is owned by exactly one task" in {
    val keys: Set[String] = (for {
      topic     <- Seq("topicA", "topicB", "topicC")
      partition <- Seq("0000001", "0000002", "0000003")
    } yield s"prefix/$topic/$partition/file.avro").toSet

    val leafDirs = keys.map(_.dropRight("file.avro".length))

    val maxTasks = 2
    val perTask = (0 until maxTasks).map { taskNo =>
      find(keys, "prefix/".some, 2, connectorTaskId = ConnectorTaskId("sourceName", maxTasks, taskNo))
    }

    perTask.reduce(_ union _) should be(leafDirs)
    perTask.foreach(_ should not be empty)
    perTask.combinations(2).foreach { case Seq(a, b) => (a intersect b) should be(Set.empty) }
  }

  "findDirectories" should "assign the single directory at depth 0 to exactly one task" in {
    val maxTasks = 2
    val perTask = (0 until maxTasks).map { taskNo =>
      find(topicPartitionKeys, "prefix/".some, 0, connectorTaskId = ConnectorTaskId("sourceName", maxTasks, taskNo))
    }

    perTask.count(_.nonEmpty) should be(1)
    perTask.reduce(_ union _) should be(Set("prefix/"))
  }
}
